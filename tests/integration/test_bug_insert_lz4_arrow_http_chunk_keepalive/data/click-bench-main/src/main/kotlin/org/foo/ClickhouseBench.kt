package org.foo

import dev.forkhandles.result4k.valueOrNull
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.produce
import org.foo.clickhouse.DatabaseCredentials
import org.foo.clickhouse.DatabaseCredentials.Companion.localCredentials
import org.foo.clickhouse.ClickHouseResultStore
import org.foo.clickhouse.ClickhouseConnectionDetails
import org.foo.clickhouse.ClickhouseConnectionDetails.Companion.localClickhouse
import org.foo.clickhouse.ClickhouseTableName
import org.foo.clickhouse.clickhouseService
import org.foo.domain.FlattenedGoatResult
import org.foo.domain.FlattenedGoatResult.Companion.fullyPopulatedResult
import org.foo.domain.JobId
import org.foo.domain.ResultStoreKey
import org.foo.domain.generateId
import java.time.Clock
import java.time.Instant
import kotlin.math.min
import kotlin.time.Duration.Companion.seconds

fun main() {
    val databaseName = "goat_stress_test_arrow"
    val tableName = ClickhouseTableName("clickhouse_bench")
    // fixed batch size of 10_000 doesn't trigger the issue
    // runTestFor(localClickhouse, localCredentials, databaseName, tableName, 4_000_000, IntRange(10_000, 10_000))

    // batches with various size trigger clickhouse connection issue when async insert = false
    runTestFor(localClickhouse, localCredentials, databaseName, tableName, 4_000_000, IntRange(5_000, 10_000))
    println("done with test")
}

private fun runTestFor(
    connectionDetails: ClickhouseConnectionDetails,
    databaseCredentials: DatabaseCredentials,
    databaseName: String,
    tableName: ClickhouseTableName,
    totalNumberOfRowsPerJob: Int,
    batchSizeRange: IntRange
) {
    clickhouseService(connectionDetails, databaseCredentials, databaseName, tableName).start().use { resultsDatabase ->
        val jobId = JobId()
        val resultSample = fullyPopulatedResult()
        val resultStoreKey = ResultStoreKey(jobId, resultSample.partition)
        val resultCount = runBlocking {
            ClickHouseResultStore(
                resultsDatabase.resultsClient(),
                Clock.systemUTC(),
                resultsTableName = tableName,
                asyncInserts = false
            ).use { clickHouseResultStore ->
                try {
                    ClickHouseStressTest(clickHouseResultStore, totalNumberOfRowsPerJob, batchSizeRange)
                        .run(resultStoreKey, resultSample)
                } finally {
                    println("dropping table")
                    resultsDatabase.resultsClient().execute("DROP TABLE IF EXISTS $databaseName.${tableName.value}")
                    println("done dropping table")
                }
            }

        }
        if (resultCount != totalNumberOfRowsPerJob) throw RuntimeException("some results are missing, expected $totalNumberOfRowsPerJob but was $resultCount; Please check failures from logs.")
    }
}

class ClickHouseStressTest(
    private val resultStore: ClickHouseResultStore,
    private val totalNumberOfRowsPerJob: Int,
    private val insertBatchSize: IntRange
) {
    suspend fun run(resultStoreKey: ResultStoreKey, sample: FlattenedGoatResult): Int {
        val parentScope = CoroutineScope(Dispatchers.IO + SupervisorJob() + CoroutineName("ClickHouseStressTest"))
        parentScope.launchProduceResultsAndWriteToDb(sample, resultStoreKey, 128)
        val count = parentScope.asyncCountResultsContinuouslyUntilStable(resultStoreKey).await()
        return count
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private fun CoroutineScope.launchProduceResultsAndWriteToDb(
        sample: FlattenedGoatResult,
        resultStoreKey: ResultStoreKey,
        numberOfWriters: Int
    ): List<Job> {
        val resultsChannel = produce {
            var remaining = totalNumberOfRowsPerJob
            while (remaining > 0 && isActive) {
                val size = min(insertBatchSize.random(), remaining)
                send((1..size).map { sample.copy(ttlDays = 1, c6 = generateId().toString()) })
                remaining -= size
            }
            this.close()
        }
        return (1..numberOfWriters).map {
            this.launch {
                for (resultsToInsert in resultsChannel) {
                    resultStore.bulkStoreFlatResult(resultStoreKey.jobId, resultsToInsert)
                }
            }
        }
    }

    private fun CoroutineScope.asyncCountResultsContinuouslyUntilStable(resultStoreKey: ResultStoreKey): Deferred<Int> {
        val delayForEachLoop = 20.seconds
        return async {
            delay(delayForEachLoop)
            var rowCount = 0
            while (isActive) {
                val newCount = resultStore.resultCountFor(resultStoreKey).valueOrNull()?.count
                println("${Instant.now()} results count=$newCount")
                if (newCount == rowCount) {
                    /* nothing inserted in the past iteration */
                    println("${Instant.now()} nothing new inserted after ${delayForEachLoop}; breaking"); break
                }
                rowCount = newCount ?: 0
                if (rowCount == totalNumberOfRowsPerJob) break
                delay(delayForEachLoop)
            }
            println("${Instant.now()} Finished counting persisted rows for ${resultStoreKey.jobId.value}; total=${rowCount}")
            rowCount
        }
    }
}
