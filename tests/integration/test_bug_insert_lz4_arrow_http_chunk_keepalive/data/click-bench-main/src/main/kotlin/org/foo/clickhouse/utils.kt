package org.foo.clickhouse

import com.fasterxml.jackson.databind.JsonNode
import dev.forkhandles.result4k.Result
import dev.forkhandles.result4k.Result4k
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.RootAllocator
import org.foo.arrow.ArrowSerializer
import org.foo.utils.ResultsDatabase
import org.foo.utils.Service
import java.io.InputStream
import java.time.Clock

@JvmInline
value class ClickhouseQueryId(val value: String)
sealed interface ClickHouseResponse {
    val queryId: ClickhouseQueryId
}
data class ClickHouseExecuteResponse(override val queryId: ClickhouseQueryId) : ClickHouseResponse
data class ClickHouseError(val queryId: ClickhouseQueryId, val exception: Exception) : Exception(exception)
data class ClickHouseInsertResponse(override val queryId: ClickhouseQueryId) : ClickHouseResponse
data class ClickHouseResultCountResponse(override val queryId: ClickhouseQueryId, val count: Int) : ClickHouseResponse
sealed interface ClickHouseSystemSyncResponse {
    data object NoSystemSyncRequired : ClickHouseSystemSyncResponse
    data class SystemSyncExecuted(override val queryId: ClickhouseQueryId) : ClickHouseSystemSyncResponse, ClickHouseResponse
}

enum class ClickHouseResponseFormat(val clickHouseValue: String) {
    NDJSON("JSONEachRow") {
        override fun additionalHeaders(): Map<String, String> = mapOf("Accept-Encoding" to "gzip")
    },
    ;

    open fun additionalQueryParameters() = emptyMap<String, String>()
    open fun additionalHeaders() = emptyMap<String, String>()
}

interface ClickHouseQueryResponse : ClickHouseResponse {
    override val queryId: ClickhouseQueryId
    fun rows(): Flow<JsonNode>
    fun toInputStream(): InputStream
    fun asJsonObject(): JsonNode
    val responseFormat : ClickHouseResponseFormat
}


interface ClickHouseClient : AutoCloseable {
    val defaultDatabase: String
    suspend fun systemSync(tableName: String): Result<ClickHouseSystemSyncResponse, ClickHouseError>
    suspend fun execute(ddl: String, settings: Map<String, String> = emptyMap()): Result<ClickHouseExecuteResponse, ClickHouseError>
    suspend fun query(
        query: String,
        responseFormat: ClickHouseResponseFormat = ClickHouseResponseFormat.NDJSON,
        settings: Map<String, String> = emptyMap(),
    ): Result<ClickHouseQueryResponse, ClickHouseError>

    suspend fun <T> insert(
        tableName: String,
        overrideSettings: Map<String, String> = emptyMap(),
        allocator: RootAllocator,
        serializer: ArrowSerializer<T>,
        items: List<T>
    ): Result4k<ClickHouseInsertResponse, ClickHouseError>

    companion object {
        val QUERY_SETTING_SELECT_SEQUENTIAL_CONSISTENCY = mapOf("select_sequential_consistency" to "1")
    }
}

fun clickhouseService(connectionDetails: ClickhouseConnectionDetails, databaseCredentials: DatabaseCredentials, databaseName: String, tableName: ClickhouseTableName) = object : Service<ResultsDatabase> {
    val clickHouseDatabaseService = createClickhouseService(
        connectionDetails,
        databaseName = databaseName,
        tableName = tableName,
        databaseCredentials = databaseCredentials
    )

    override fun start(): ResultsDatabase {
        val resultsDatabase = clickHouseDatabaseService.start()
        runBlocking { resultsDatabase.migrate() }
        return resultsDatabase
    }
}

fun createClickhouseService(
    connectionDetails: ClickhouseConnectionDetails,
    databaseName: String,
    tableName: ClickhouseTableName,
    databaseCredentials: DatabaseCredentials
): ClickHouseDatabaseService = ClickHouseDatabaseService(
    clickhouseConnectionDetails = connectionDetails,
    databaseName = databaseName,
    credentials = { databaseCredentials },
    tableName,
    Clock.systemUTC(),
)

class ClickHouseDatabaseService(
    private val clickhouseConnectionDetails: ClickhouseConnectionDetails,
    private val databaseName: String,
    private val credentials: () -> DatabaseCredentials,
    private val resultsTableName: ClickhouseTableName,
    private val clock: Clock,
    private val retryOnConnectionFailure: Boolean = true,
) : Service<ResultsDatabase> {

    override fun start(): ResultsDatabase {
        return object : ResultsDatabase {
            val okHttpClient = OkHttpClickHouseClient(
                connectionDetails = clickhouseConnectionDetails,
                credentials = credentials(),
                defaultDatabase = databaseName,
                clock = clock,
                retryOnConnectionFailure = retryOnConnectionFailure
            )

            override suspend fun migrate() {
                ClickHouseSchemaCreator(
                    databaseName,
                    okHttpClient,
                    clickhouseConnectionDetails.canCreateDatabase,
                    resultsTableName
                ).create()
            }

            override fun resultsClient(): ClickHouseClient = okHttpClient

            override fun close() {
                okHttpClient.close()
            }
        }
    }

}
