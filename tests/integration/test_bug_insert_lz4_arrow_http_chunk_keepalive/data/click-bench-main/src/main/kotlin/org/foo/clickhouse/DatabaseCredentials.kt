package org.foo.clickhouse

data class DatabaseCredentials(val username: String, val password: String) {
    companion object {
        val localCredentials  = DatabaseCredentials("default", "")
    }
}
