package org.foo.okhttp

import okhttp3.*
import org.slf4j.Logger
import java.io.IOException
import java.time.Clock
import java.time.Duration
import java.time.Instant

class AuditingOkHttpMetricsListener(
    private val clock: Clock = Clock.systemUTC(),
    private val logger: Logger
) : EventListener() {
    private val createdAt = Instant.now(clock)
    private var start: Instant? = null
    private var responseCode: Int? = null

    override fun connectionAcquired(call: Call, connection: Connection) {
        val now = Instant.now(clock)
        val socket = connection.socket()
        val request = call.request()
        val queuingTime = Duration.between(createdAt, now)
        logger.info("call started: using socket local=${socket.localAddress}:${socket.localPort} remote=${socket.inetAddress}:${socket.port}; uri=${request.url} headers=${request.getHeaders()} ${queuingTime}")
        if (start == null) {
            start = now
        }
    }

    override fun responseHeadersEnd(call: Call, response: Response) {
        responseCode = response.code
    }

    override fun callFailed(call: Call, ioe: IOException) {
        val now = Instant.now(clock)
        val request = call.request()
        logger.error("called failed executionTime=${Duration.between(createdAt, now)} url=${call.request().url} responseCode=${this.responseCode} method=${call.request().method} headers=${request.getHeaders()}" , ioe)
    }

    override fun callEnd(call: Call) {
        val now = Instant.now(clock)
        val request = call.request()
        logger.info("called end executionTime=${Duration.between(createdAt, now)} url=${call.request().url} responseCode=${this.responseCode} method=${call.request().method} headers=${request.getHeaders()}" )
    }

    private fun Request.getHeaders(): String =
        headers.map { (key, value) -> if (key == "Authorization") "${key}=redacted" else "${key}=${value}" }
            .toString()

}
