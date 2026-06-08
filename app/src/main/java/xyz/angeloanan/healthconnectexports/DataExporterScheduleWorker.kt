package xyz.angeloanan.healthconnectexports

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.content.Context
import android.util.Log
import androidx.core.app.NotificationCompat
import androidx.core.content.getSystemService
import androidx.health.connect.client.HealthConnectClient
import androidx.health.connect.client.permission.HealthPermission
import androidx.health.connect.client.records.ActiveCaloriesBurnedRecord
import androidx.health.connect.client.records.DistanceRecord
import androidx.health.connect.client.records.ExerciseSessionRecord
import androidx.health.connect.client.records.HeartRateRecord
import androidx.health.connect.client.records.OxygenSaturationRecord
import androidx.health.connect.client.records.SleepSessionRecord
import androidx.health.connect.client.records.StepsRecord
import androidx.health.connect.client.records.TotalCaloriesBurnedRecord
import androidx.health.connect.client.request.AggregateRequest
import androidx.health.connect.client.request.ReadRecordsRequest
import androidx.health.connect.client.time.TimeRangeFilter
import androidx.work.CoroutineWorker
import androidx.work.WorkerParameters
import com.google.gson.Gson
import io.ktor.client.HttpClient
import io.ktor.client.engine.android.Android
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import java.time.Instant
import java.time.LocalDate
import java.util.TimeZone

val httpClient = HttpClient(Android)

val requiredHealthConnectPermissions = setOf(
    HealthPermission.getReadPermission(StepsRecord::class),
    HealthPermission.getReadPermission(SleepSessionRecord::class),
    HealthPermission.getReadPermission(ActiveCaloriesBurnedRecord::class),
    HealthPermission.getReadPermission(TotalCaloriesBurnedRecord::class),
    HealthPermission.getReadPermission(HeartRateRecord::class),
    HealthPermission.getReadPermission(OxygenSaturationRecord::class),
    HealthPermission.getReadPermission(DistanceRecord::class),
    HealthPermission.getReadPermission(ExerciseSessionRecord::class),
)

class DataExporterScheduleWorker(appContext: Context, workerParams: WorkerParameters) : CoroutineWorker(appContext, workerParams) {
    private val notificationManager = applicationContext.getSystemService<NotificationManager>()!!
    private val healthConnect = HealthConnectClient.getOrCreate(applicationContext)

    private fun createNotificationChannel(): NotificationChannel {
        val notificationChannel = NotificationChannel("export", "Data export", NotificationManager.IMPORTANCE_LOW)
        notificationChannel.description = "Shown when Health Connect data is being exported"
        notificationChannel.enableLights(false)
        notificationChannel.enableVibration(false)
        notificationManager.createNotificationChannel(notificationChannel)
        return notificationChannel
    }

    private fun createExceptionNotification(e: Exception): Notification {
        return NotificationCompat.Builder(applicationContext, "export")
            .setContentTitle("Export failed")
            .setContentText("Failed to export Health Connect data")
            .setStyle(NotificationCompat.BigTextStyle().bigText(e.message))
            .setSmallIcon(R.drawable.ic_launcher_foreground)
            .build()
    }

    private suspend fun isHealthConnectPermissionGranted(healthConnect: HealthConnectClient): Boolean {
        val grantedPermissions = healthConnect.permissionController.getGrantedPermissions()
        return requiredHealthConnectPermissions.all { it in grantedPermissions }
    }

    private suspend fun readHeartRateSummary(start: Instant, end: Instant): Map<String, Any?> {
        val records = healthConnect.readRecords(ReadRecordsRequest(recordType = HeartRateRecord::class, timeRangeFilter = TimeRangeFilter.between(start, end))).records
        val samples = records.flatMap { record -> record.samples.map { sample -> mapOf("time" to sample.time.toEpochMilli(), "beats_per_minute" to sample.beatsPerMinute) } }.sortedBy { it["time"] as Long }
        val values = samples.mapNotNull { it["beats_per_minute"] as? Long }
        return mapOf("sample_count" to samples.size, "latest" to samples.lastOrNull(), "min_bpm" to values.minOrNull(), "max_bpm" to values.maxOrNull(), "avg_bpm" to values.takeIf { it.isNotEmpty() }?.average())
    }

    private suspend fun readOxygenSaturationSummary(start: Instant, end: Instant): Map<String, Any?> {
        val records = healthConnect.readRecords(ReadRecordsRequest(recordType = OxygenSaturationRecord::class, timeRangeFilter = TimeRangeFilter.between(start, end))).records
        val samples = records.map { record -> mapOf("time" to record.time.toEpochMilli(), "percentage" to record.percentage.value) }.sortedBy { it["time"] as Long }
        val values = samples.mapNotNull { it["percentage"] as? Double }
        return mapOf("sample_count" to samples.size, "latest" to samples.lastOrNull(), "min_percentage" to values.minOrNull(), "max_percentage" to values.maxOrNull(), "avg_percentage" to values.takeIf { it.isNotEmpty() }?.average())
    }

    private suspend fun readExerciseSessions(start: Instant, end: Instant): List<Map<String, Any?>> {
        val records = healthConnect.readRecords(ReadRecordsRequest(recordType = ExerciseSessionRecord::class, timeRangeFilter = TimeRangeFilter.between(start, end))).records
        return records.map { record -> mapOf("start_time" to record.startTime.toEpochMilli(), "end_time" to record.endTime.toEpochMilli(), "duration_seconds" to java.time.Duration.between(record.startTime, record.endTime).seconds, "exercise_type" to record.exerciseType, "title" to record.title, "notes" to record.notes) }.sortedBy { it["start_time"] as Long }
    }

    override suspend fun doWork(): Result {
        val notificationChannel = createNotificationChannel()
        val isGranted = isHealthConnectPermissionGranted(healthConnect)
        if (!isGranted) return Result.failure()
        val exportDestination: String? = applicationContext.dataStore.data.map { it[EXPORT_DESTINATION_URI] }.first()
        if (exportDestination == null) return Result.failure()
        val foregroundNotification = NotificationCompat.Builder(applicationContext, notificationChannel.id).setContentTitle("Exporting data").setContentText("Exporting Health Connect data to the cloud").setSmallIcon(R.drawable.ic_launcher_foreground).setOngoing(true).build()
        notificationManager.notify(1, foregroundNotification)

        val zoneId = TimeZone.getDefault().toZoneId()
        val startOfDay = LocalDate.now(zoneId).atStartOfDay(zoneId).minusDays(1).toInstant()
        val endOfDay = LocalDate.now(zoneId).atStartOfDay(zoneId).toInstant().minusMillis(1)
        val healthDataAggregate = healthConnect.aggregate(AggregateRequest(metrics = setOf(StepsRecord.COUNT_TOTAL, ActiveCaloriesBurnedRecord.ACTIVE_CALORIES_TOTAL, TotalCaloriesBurnedRecord.ENERGY_TOTAL, SleepSessionRecord.SLEEP_DURATION_TOTAL, DistanceRecord.DISTANCE_TOTAL), timeRangeFilter = TimeRangeFilter.between(startOfDay, endOfDay)))
        val jsonValues = HashMap<String, Any?>()
        jsonValues["steps"] = healthDataAggregate[StepsRecord.COUNT_TOTAL] ?: 0
        jsonValues["active_calories"] = healthDataAggregate[ActiveCaloriesBurnedRecord.ACTIVE_CALORIES_TOTAL]?.inKilocalories ?: 0
        jsonValues["total_calories"] = healthDataAggregate[TotalCaloriesBurnedRecord.ENERGY_TOTAL]?.inKilocalories ?: 0
        jsonValues["sleep_duration_seconds"] = healthDataAggregate[SleepSessionRecord.SLEEP_DURATION_TOTAL]?.seconds ?: 0
        jsonValues["distance_meters"] = healthDataAggregate[DistanceRecord.DISTANCE_TOTAL]?.inMeters ?: 0
        jsonValues["heart_rate"] = readHeartRateSummary(startOfDay, endOfDay)
        jsonValues["oxygen_saturation"] = readOxygenSaturationSummary(startOfDay, endOfDay)
        jsonValues["exercise_sessions"] = readExerciseSessions(startOfDay, endOfDay)
        val json = Gson().toJson(mapOf("time" to startOfDay.toEpochMilli(), "range" to mapOf("start" to startOfDay.toEpochMilli(), "end" to endOfDay.toEpochMilli(), "timezone" to zoneId.id), "source" to "health_connect", "data" to jsonValues))
        try {
            val destination = if (exportDestination.startsWith("http://") || exportDestination.startsWith("https://")) exportDestination else "https://$exportDestination"
            httpClient.post(destination) { contentType(ContentType.Application.Json); setBody(json) }
        } catch (e: Exception) {
            Log.e("DataExporterWorker", "Failed to export data", e)
            notificationManager.cancel(1)
            notificationManager.notify(1, createExceptionNotification(e))
            return Result.failure()
        }
        notificationManager.cancel(1)
        return Result.success()
    }
}