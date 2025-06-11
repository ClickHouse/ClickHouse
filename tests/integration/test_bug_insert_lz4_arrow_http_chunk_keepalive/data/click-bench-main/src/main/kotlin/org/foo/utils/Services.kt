package org.foo.utils

import org.foo.clickhouse.ClickHouseClient

interface Service<T : AutoCloseable> {
    fun start() : T
}

interface ResultsDatabase : AutoCloseable {
    suspend fun migrate()
    fun resultsClient(): ClickHouseClient
}

