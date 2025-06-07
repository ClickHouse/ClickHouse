package org.foo.clickhouse

import net.sourceforge.urin.Authority
import net.sourceforge.urin.Authority.authority
import net.sourceforge.urin.Host.registeredName
import net.sourceforge.urin.Port.port
import java.net.URI

interface ClientClickhouseConnectionDetails {
    val uri: URI
    val systemSyncRequired: Boolean
}

interface ClickhouseConnectionDetails : ClientClickhouseConnectionDetails {
    val authority: Authority
    override val uri: URI
    val useSsl: Boolean
    val canCreateDatabase: Boolean
    override val systemSyncRequired: Boolean

    data class SelfHostedClickhouseConnectionDetails(override val authority: Authority, override val uri: URI) : ClickhouseConnectionDetails {
        constructor(hostName: String, port: Int) : this(authority(registeredName(hostName), port(port)), URI.create("http://${hostName}:${port}"))

        override val useSsl = false
        override val canCreateDatabase = true
        override val systemSyncRequired = false
    }

    companion object {
        val localClickhouse = SelfHostedClickhouseConnectionDetails(
            System.getenv("CLICKHOUSE_HOST") ?: "localhost",
            System.getenv("CLICKHOUSE_PORT")?.toIntOrNull() ?: 8123
        )
    }

}
