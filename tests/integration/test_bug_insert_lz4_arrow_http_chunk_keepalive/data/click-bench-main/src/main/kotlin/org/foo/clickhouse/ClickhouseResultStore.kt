package org.foo.clickhouse

import dev.forkhandles.result4k.*
import kotlinx.coroutines.flow.toList
import org.foo.arrow.ArrowSerializerUtils
import org.foo.clickhouse.ClickHouseClient.Companion.QUERY_SETTING_SELECT_SEQUENTIAL_CONSISTENCY
import org.foo.clickhouse.ColumnName.PartitionColumn
import org.foo.clickhouse.SchemaProvider.Companion.buildSchema
import org.foo.domain.FlattenedGoatResult
import org.foo.domain.JobId
import org.foo.domain.ResultStoreKey
import java.time.Clock
import java.time.Instant

class ClickHouseResultStore(
    private val clickHouseClient: ClickHouseClient,
    private val clock: Clock,
    private val asyncInserts: Boolean = false,
    private val resultsTableName: ClickhouseTableName
): AutoCloseable {
    private val rootAllocator = ArrowSerializerUtils.rootAllocator("root-result-store")
    private val threeGigabytesInBytes = "3221225472"

    private val schema = buildSchema()

    suspend fun bulkStoreFlatResult(jobId: JobId, flattenedGoatResults: List<FlattenedGoatResult>) {
        val settings = if (asyncInserts) {
            mapOf("wait_for_async_insert" to "0", "async_insert_max_data_size" to threeGigabytesInBytes)
        } else {
            emptyMap()
        }
        clickHouseClient.insert(
            resultsTableName.value,
            settings,
            rootAllocator,
            ResultsArrowSerializer(jobId, clock, schema),
            flattenedGoatResults
        ).peekFailure {
            println("${Instant.now()} - Cannot insert batch of size ${flattenedGoatResults.size} ${it.stackTraceToString()}")
        }
    }

    suspend fun resultCountFor(resultStoreKey: ResultStoreKey): Result<ClickHouseResultCountResponse, ClickHouseError> {
        val jobId = JobIdSchemaV3.equalClause(resultStoreKey.jobId)

        val query = """
            select count() as resultCount
            from ${resultsTableName.value}
            where ${ColumnName.JobIdForUintColumn.value} = $jobId and ${PartitionColumn.value} = '${resultStoreKey.seriesId}'
        """.trimIndent()

        return clickHouseClient.query(query, settings = QUERY_SETTING_SELECT_SEQUENTIAL_CONSISTENCY)
            .map { queryResponse ->
                ClickHouseResultCountResponse(queryResponse.queryId, queryResponse.rows().toList().first()["resultCount"].intValue())
            }
    }

    suspend fun systemSync() {
        clickHouseClient.systemSync(resultsTableName.value)
            .onFailure { throw RuntimeException("ResultStore. Failed to run systemSync, ${it.reason.exception.message}", it.reason.exception) }
    }

    override fun close() {
        clickHouseClient.close()
        rootAllocator.close()
    }

}
