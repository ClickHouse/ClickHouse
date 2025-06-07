package org.foo.clickhouse

import com.fasterxml.jackson.databind.JsonNode
import dev.forkhandles.result4k.*
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import net.jpountz.lz4.LZ4FrameOutputStream
import okhttp3.*
import okhttp3.Credentials.basic
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import okio.BufferedSink
import okio.use
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.http4k.format.Jackson.asJsonObject
import org.foo.arrow.ArrowSerializer
import org.foo.arrow.ArrowVectors
import org.foo.okhttp.AuditingOkHttpMetricsListener
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.channels.Channels
import java.time.Clock
import java.time.Duration
import java.util.*
import java.util.zip.GZIPInputStream

fun interface QueryIdGenerator {
    fun generate(): ClickhouseQueryId
}

class OkHttpClickHouseClient(
    private val maxRequest: Int = 64,
    callTimeout: Duration = Duration.ofSeconds(300),
    readWriteTimeout: Duration = Duration.ofSeconds(600),
    private val connectionDetails: ClientClickhouseConnectionDetails,
    private val credentials: DatabaseCredentials,
    override val defaultDatabase: String,
    private val queryIdGenerator: QueryIdGenerator = QueryIdGenerator {
        ClickhouseQueryId(
            UUID.randomUUID().toString()
        )
    },
    clock: Clock,
    retryOnConnectionFailure: Boolean = false
) : AutoCloseable, ClickHouseClient {

    private val logger = LoggerFactory.getLogger(OkHttpClickHouseClient::class.java)

    private val dispatcher = Dispatcher().apply {
        this.maxRequestsPerHost = maxRequest
        this.maxRequests = maxRequest
    }

    private val client: OkHttpClient =
        OkHttpClient.Builder()
            .callTimeout(callTimeout)
            .readTimeout(readWriteTimeout)
            .writeTimeout(readWriteTimeout)
            .connectTimeout(callTimeout)
            .followRedirects(false)
            .eventListenerFactory { AuditingOkHttpMetricsListener(clock, logger) }
            .dispatcher(dispatcher)
            .retryOnConnectionFailure(retryOnConnectionFailure)
            .build()

    override suspend fun systemSync(tableName: String) = if (connectionDetails.systemSyncRequired) {
        execute("SYSTEM SYNC REPLICA ON CLUSTER default $defaultDatabase.$tableName LIGHTWEIGHT")
            .map { ClickHouseSystemSyncResponse.SystemSyncExecuted(it.queryId) }
    } else {
        ClickHouseSystemSyncResponse.NoSystemSyncRequired.asSuccess()
    }

    override suspend fun execute(
        ddl: String,
        settings: Map<String, String>
    ): Result<ClickHouseExecuteResponse, ClickHouseError> {
        val queryId = queryIdGenerator.generate()

        val queryBuilder = connectionDetails.uri.toHttpUrlOrNull()?.newBuilder() ?: return ClickHouseError(
            queryId,
            IllegalStateException("The url was not http/https: ${connectionDetails.uri}")
        ).asFailure()

        queryBuilder.addQueryParameter("query_id", queryId.value)

        settings.forEach { (setting, value) -> queryBuilder.addQueryParameter(setting, value) }

        val request = Request.Builder()
            .post(ddl.toRequestBody())
            .url(queryBuilder.build().toUrl())
            .header("Content-Type", "text/plain; charset=utf-8")
            .header("Authorization", basic(this.credentials.username, this.credentials.password))
            .build()

        return makeTheCall(
            request,
            successResponse = { response -> response.use { ClickHouseExecuteResponse(queryId) } },
            failureResponse = { response: Response ->
                response.use {
                    ClickHouseError(
                        queryId,
                        UnsuccessfulClickHouseResponseException(
                            response.code,
                            response.headers.toString(),
                            response.toStringBody()
                        )
                    )
                }
            },
            error = { exception -> ClickHouseError(queryId, exception) }
        )
    }

    override suspend fun query(
        query: String,
        responseFormat: ClickHouseResponseFormat,
        settings: Map<String, String>,
    ): Result<ClickHouseQueryResponse, ClickHouseError> {
        val queryId = queryIdGenerator.generate()

        val queryBuilder = connectionDetails.uri.toHttpUrlOrNull()?.newBuilder() ?: return ClickHouseError(
            queryId,
            IllegalStateException("The url was not http/https: ${connectionDetails.uri}")
        ).asFailure()

        queryBuilder
            .addQueryParameter("output_format_json_quote_64bit_integers", "0")
            .addQueryParameter("default_format", responseFormat.clickHouseValue)
            .addQueryParameter("enable_http_compression", "1")
            .addQueryParameter("date_time_output_format", "iso")
            .addQueryParameter("use_concurrency_control", "0")
            .addQueryParameter("query_id", queryId.value)

        responseFormat.additionalQueryParameters()
            .forEach { (name, value) -> queryBuilder.addQueryParameter(name, value) }
        settings.forEach { (setting, value) -> queryBuilder.addQueryParameter(setting, value) }

        val request = responseFormat.additionalHeaders().entries.fold(Request.Builder()) { acc, entry ->
            acc.addHeader(entry.key, entry.value)
        }.post(query.toRequestBody())
            .url(queryBuilder.build().toUrl())
            .header("X-ClickHouse-Database", defaultDatabase)
            .header("Content-Type", "text/plain; charset=utf-8")
            .header("Authorization", basic(this.credentials.username, this.credentials.password))
            .build()

        return makeTheCall(
            request,
            successResponse = { response ->
                OkHttpClickHouseQueryResponse(queryId, response, responseFormat)
            },
            failureResponse = { response ->
                response.use {
                    ClickHouseError(
                        queryId,
                        UnsuccessfulClickHouseResponseException(
                            response.code,
                            response.headers.toString(),
                            response.toStringBody()
                        )
                    )
                }
            },
            error = { exception -> ClickHouseError(queryId, exception) }
        )
    }

    override suspend fun <T> insert(
        tableName: String,
        overrideSettings: Map<String, String>,
        allocator: RootAllocator,
        serializer: ArrowSerializer<T>,
        items: List<T>
    ): Result4k<ClickHouseInsertResponse, ClickHouseError> {
        val queryId = queryIdGenerator.generate()
        val queryBuilder = connectionDetails.uri.toHttpUrlOrNull()?.newBuilder() ?: return ClickHouseError(
            queryId,
            IllegalStateException("The url was not http/https: ${connectionDetails.uri}")
        ).asFailure()

        val defaultSettings = mapOf(
            "query" to "INSERT INTO $tableName FORMAT ArrowStream",
            "async_insert" to "1",
            "wait_for_async_insert" to "1",
            "date_time_input_format" to "best_effort",
            "query_id" to queryId.value,
        )

        (defaultSettings + overrideSettings).forEach { queryBuilder.addQueryParameter(it.key, it.value) }

        val body = ArrowStreamRequestBody(allocator, serializer, items, queryId, logger)
        val request = Request.Builder()
            .post(body)
            .url(queryBuilder.build().toUrl())
            .header("X-ClickHouse-Database", defaultDatabase)
            .header("Content-Encoding", "lz4")
            .header("Content-Type", body.contentType().toString())
            .header("Authorization", basic(this.credentials.username, this.credentials.password))
            .build()

        logger.info("queryId=${queryId.value} before okhttp call; itemsCount=${items.count()}")
        return makeTheCall(
            request,
            successResponse = { response ->
                logger.info("queryId=${queryId.value} okhttp call successful; size=${items.count()}")
                response.use { ClickHouseInsertResponse(queryId) }
            },
            failureResponse = { response ->
                response.use {
                    val headers = response.headers.toString()
                    val failure = response.toStringBody()
                    logger.error("queryId=${queryId.value} okhttp call failed; itemsCount=${items.count()} headers=$headers failure=${failure}")
                    ClickHouseError(
                        queryId,
                        UnsuccessfulClickHouseResponseException(response.code, headers, failure)
                    )
                }
            },
            error = { exception -> ClickHouseError(queryId, exception) }
        )
    }


    private suspend fun <T : ClickHouseResponse> makeTheCall(
        request: Request,
        successResponse: (response: Response) -> T,
        failureResponse: (response: Response) -> ClickHouseError,
        error: (exception: IOException) -> ClickHouseError
    ): Result<T, ClickHouseError> {
        val deferred = CompletableDeferred<Result<T, ClickHouseError>>()
        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                deferred.complete(error(e).asFailure())
            }

            override fun onResponse(call: Call, response: Response) {
                if (response.isSuccessful) {
                    deferred.complete(successResponse(response).asSuccess())
                } else {
                    deferred.complete(failureResponse(response).asFailure())
                }
            }
        })
        return deferred.await()
    }

    override fun close() {}
}

class StreamingArrowWriter<T>(
    private val logger: Logger,
    private val allocator: BufferAllocator,
    private val serializer: ArrowSerializer<T>,
) {

    fun writeStream(data: List<T>, queryId: ClickhouseQueryId, sink: OutputStream) {
        VectorSchemaRoot.create(serializer.schema, allocator).use { schema ->
            val vectors: ArrowVectors = serializer.initVectors(schema)
            ArrowStreamWriter(schema, null, Channels.newChannel(sink)).use { arrowStreamWriter ->
                logger.info("queryId=${queryId.value} arrowStreamWriter.start()")
                arrowStreamWriter.start()
                vectors.allocateNew(data.size)
                logger.info("queryId=${queryId.value} writing data ${data.size}")
                data.forEachIndexed { index, item ->
                    serializer.populate(item, vectors, index)
                }
                schema.rowCount = data.size
                arrowStreamWriter.writeBatch()
                schema.clear()
                arrowStreamWriter.end()
                logger.info("queryId=${queryId.value} finished writing elements ${schema.rowCount}")
            }

        }
    }
}

class ArrowStreamRequestBody<T>(
    private val allocator: BufferAllocator,
    private val serializer: ArrowSerializer<T>,
    private val data: List<T>,
    private val queryId: ClickhouseQueryId,
    private val logger: Logger,
) : RequestBody() {
    override fun contentType(): MediaType = "application/x-arrow".toMediaType()

    override fun writeTo(sink: BufferedSink) {
        sink.outputStream().use { out ->
            CountingOutputStream(out).use { countingStream ->
                LZ4FrameOutputStream(countingStream, LZ4FrameOutputStream.BLOCKSIZE.SIZE_4MB).use { outputStream ->
                    try {
                        logger.info("queryId=${queryId.value} writing to stream; items=${data.size}")
                        StreamingArrowWriter(logger, allocator, serializer).writeStream(data, queryId, outputStream)
                        logger.info("queryId=${queryId.value} written to stream; items=${data.size}; bytes=${countingStream.byteCount}")
                    } catch (e: Throwable) {
                        logger.error(
                            "queryId=${queryId.value} failed to write to stream; items=${data.size}; bytes=${countingStream.byteCount};",
                            e
                        )
                        throw e
                    }
                }
            }
        }
    }
}

class CountingOutputStream(private val out: OutputStream) : OutputStream() {
    var byteCount: Long = 0
        private set

    override fun write(b: Int) {
        out.write(b)
        byteCount++
    }

    override fun write(b: ByteArray) {
        out.write(b)
        byteCount += b.size
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        out.write(b, off, len)
        byteCount += len
    }

    override fun flush() = out.flush()
    override fun close() = out.close()
}

fun Response.toStringBody(): String = this.toInputStream().use { it.readBytes().toString(Charsets.UTF_8) }
fun Response.toInputStream(): InputStream {
    val isGzipped = header("Content-Encoding") == "gzip"

    return if (isGzipped) {
        GZIPInputStream(body?.byteStream())
    } else {
        body?.byteStream() ?: ByteArrayInputStream(ByteArray(0))
    }
}

class UnsuccessfulClickHouseResponseException(responseCode: Int, headers: String, failure: String) :
    RuntimeException("responseCode:$responseCode, headers:$headers, body:$failure")

data class OkHttpClickHouseQueryResponse(
    override val queryId: ClickhouseQueryId,
    val response: Response,
    override var responseFormat: ClickHouseResponseFormat
) : ClickHouseQueryResponse {
    override fun rows(): Flow<JsonNode> {
        return flow {
            response.use {
                toInputStream().use {
                    it.bufferedReader().useLines { lines ->
                        lines.forEach { line ->
                            emit(line.asJsonObject())
                        }
                    }
                }
            }
        }
    }

    override fun toInputStream(): InputStream = response.toInputStream()
    override fun asJsonObject(): JsonNode = toInputStream().use {
        it.readBytes().toString(Charsets.UTF_8).asJsonObject()
    }
}
