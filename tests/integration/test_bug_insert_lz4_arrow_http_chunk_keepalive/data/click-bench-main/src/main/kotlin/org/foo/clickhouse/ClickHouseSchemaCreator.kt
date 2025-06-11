package org.foo.clickhouse

import dev.forkhandles.result4k.onFailure

data class ClickhouseTableName(val value: String)
class ClickHouseSchemaCreator(
    private val databaseName: String,
    private val clickHouseClient: ClickHouseClient,
    private val canCreateDatabase: Boolean,
    private val resultsTableName: ClickhouseTableName
) {

    suspend fun create() {
        (listOfNotNull(if (canCreateDatabase) createDatabase else null) +
                ResultsSchema(databaseName).ddls(resultsTableName)).forEach { ddl ->
            clickHouseClient.execute(ddl).onFailure {
                throw RuntimeException(
                    "Failed to execute DDL with query id ${it.reason.queryId}: $ddl",
                    it.reason.exception
                )
            }
        }
    }

    private val createDatabase = """ CREATE DATABASE IF NOT EXISTS $databaseName; """
}

