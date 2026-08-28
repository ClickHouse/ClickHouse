#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag justification:
#   no-fasttest: needs libmysql (the MySQL database engine), which is not built in fast test.
#   no-parallel: the database attached here is deliberately unreachable and visible in
#     system.tables, so every concurrent scan without a database filter would pay its connect
#     timeout and log a warning.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Introspection that covers every database must survive one unreachable remote database, while a
# query that names that database must still report the failure. Port 1 is privileged and never
# listens, so connections are refused instantly (a blackholed address would wait for a timeout).

# The default send_logs_level=warning would stream the tolerated failures to stderr.
CLICKHOUSE_CLIENT_QUIET=$(echo "${CLICKHOUSE_CLIENT}" | sed "s/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=fatal/g")

MYSQL_DB="${CLICKHOUSE_DATABASE}_mysql"
REMOTE_DB="${CLICKHOUSE_DATABASE}_remote"
SCAN_QUERY_ID="${CLICKHOUSE_DATABASE}_scan_${RANDOM}${RANDOM}"

# Drop on any exit: a database left attached slows every later scan in the run.
trap "${CLICKHOUSE_CLIENT} -q 'DROP DATABASE IF EXISTS ${MYSQL_DB}; DROP DATABASE IF EXISTS ${REMOTE_DB}'" EXIT

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${MYSQL_DB}"
${CLICKHOUSE_CLIENT_QUIET} -q "
    ATTACH DATABASE ${MYSQL_DB} ENGINE = MySQL('127.0.0.1:1', 'fake_db', 'user', 'password')
        SETTINGS connect_timeout = 1, connection_max_tries = 1"

# Scans that name no database serve what the reachable databases hold.
echo -n 'tables '
${CLICKHOUSE_CLIENT_QUIET} --query_id "${SCAN_QUERY_ID}" -q "SELECT count() > 0 FROM system.tables"
echo -n 'columns '
${CLICKHOUSE_CLIENT_QUIET} -q "SELECT count() > 0 FROM system.columns"
echo -n 'completions '
${CLICKHOUSE_CLIENT_QUIET} -q "SELECT count() > 0 FROM system.completions"
# A predicate on `engine` does not name a database, and takes the other branch of getFilteredTables.
echo -n 'tables_by_engine '
${CLICKHOUSE_CLIENT_QUIET} -q "SELECT count() > 0 FROM system.tables WHERE engine = 'SystemTables'"

# Every other system table that enumerates all databases and lists their tables. They reach the
# best-effort `getTablesIterator`, which the engine itself makes tolerant, so they are covered by a
# different mechanism than the four scans above and need their own assertions. These tables are
# legitimately empty here, so the assertion is that the scan answers at all.
# `system.kafka_consumers` is behind `USE_RDKAFKA`: treat its absence as vacuously satisfied so the
# reference stays the same across build configurations.
for table in kafka_consumers s3_queue_settings azure_queue_settings iceberg_files iceberg_history
do
    echo -n "${table} "
    if [[ "$(${CLICKHOUSE_CLIENT_QUIET} -q \
            "SELECT count() FROM system.tables WHERE database = 'system' AND name = '${table}'")" == "0" ]]
    then
        echo 1
    else
        ${CLICKHOUSE_CLIENT_QUIET} -q "SELECT count() >= 0 FROM system.\`${table}\`"
    fi
done

# A typo must answer UNKNOWN_TABLE. The hint lookup iterates every database, so it used to answer
# the connection failure instead.
echo -n 'unknown_table '
${CLICKHOUSE_CLIENT_QUIET} -q "SELECT * FROM ${CLICKHOUSE_DATABASE}.no_such_table" 2>&1 | grep -c 'UNKNOWN_TABLE'

# A query that names the unreachable database must report the failure rather than claim the
# database has no tables.
echo -n 'named_database_fails '
${CLICKHOUSE_CLIENT_QUIET} -q "SELECT count() FROM system.tables WHERE database = '${MYSQL_DB}'" 2>&1 | grep -c 'ALL_CONNECTION_TRIES_FAILED'
echo -n 'show_tables_fails '
${CLICKHOUSE_CLIENT_QUIET} -q "SHOW TABLES FROM ${MYSQL_DB}" 2>&1 | grep -c 'ALL_CONNECTION_TRIES_FAILED'
# system.columns decides the same thing separately, so it needs its own assertion.
echo -n 'named_database_columns_fails '
${CLICKHOUSE_CLIENT_QUIET} -q "SELECT count() FROM system.columns WHERE database = '${MYSQL_DB}'" 2>&1 | grep -c 'ALL_CONNECTION_TRIES_FAILED'

# The `Remote` engine lists its tables from the remote ClickHouse server, so an unreachable server
# is the same class of failure and goes through the same mechanism. It wraps a failure to reach every
# replica into `NO_REMOTE_SHARD_AVAILABLE`, which is what its classifier tolerates.
${CLICKHOUSE_CLIENT_QUIET} -q "DROP DATABASE IF EXISTS ${REMOTE_DB}"
${CLICKHOUSE_CLIENT_QUIET} -q "CREATE DATABASE ${REMOTE_DB} ENGINE = Remote('127.0.0.1:1', 'fake_db', 'user', 'password')"
echo -n 'remote_engine_scan '
${CLICKHOUSE_CLIENT_QUIET} -q "SELECT count() > 0 FROM system.tables"
echo -n 'remote_engine_named_database_fails '
${CLICKHOUSE_CLIENT_QUIET} -q "SHOW TABLES FROM ${REMOTE_DB}" 2>&1 | grep -c 'NO_REMOTE_SHARD_AVAILABLE'

# The tolerated failure is a warning: Error-level messages from these paths trip the Upgrade check.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT 'scan_no_error', count() = 0 FROM system.text_log
        WHERE query_id = '${SCAN_QUERY_ID}' AND level = 'Error';
    SELECT 'scan_warning_seen', count() >= 1 FROM system.text_log
        WHERE query_id = '${SCAN_QUERY_ID}' AND logger_name = 'ListTables' AND level = 'Warning';
"
