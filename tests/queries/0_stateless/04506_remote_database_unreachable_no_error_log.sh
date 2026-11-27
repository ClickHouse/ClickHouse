#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag justification:
#   no-fasttest: needs libpq and libmysql, which are not built in fast test.
#   no-parallel: the databases created here point at an unreachable host and are visible in
#     system.tables, so a concurrent scan of it without a database filter would connect there too.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A tolerated connection failure to an unreachable remote database must be logged at Warning, not
# Error: Error-level messages from these paths trip the Upgrade check on unrelated PRs. Port 1 is
# privileged and never listens, so connections are refused instantly (a blackholed address would
# instead wait for a timeout).

# The default send_logs_level=warning would stream the tolerated failures to stderr and fail the test.
CLICKHOUSE_CLIENT_QUIET=$(echo "${CLICKHOUSE_CLIENT}" | sed "s/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=fatal/g")

PG_DB="${CLICKHOUSE_DATABASE}_pg"
MYSQL_DB="${CLICKHOUSE_DATABASE}_mysql"
MYSQL_ATTACH_QUERY_ID="${CLICKHOUSE_DATABASE}_mysql_attach_${RANDOM}${RANDOM}"
PG_SCAN_QUERY_ID="${CLICKHOUSE_DATABASE}_pg_scan_${RANDOM}${RANDOM}"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${PG_DB}"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${MYSQL_DB}"

# The background cleaner has no query_id, so its text_log rows are isolated by time instead.
# Microsecond precision is needed to tell them from a stale row written by an earlier run of this
# test within the same second.
RUN_START=$(${CLICKHOUSE_CLIENT} -q "SELECT toUnixTimestamp64Micro(now64(6))")

# PostgreSQL database engine does not connect at CREATE time, so an unreachable host is fine
# here. CREATE also schedules the background cleaner task (removeOutdatedTables), which fails
# to connect and logs the tolerated failure.
${CLICKHOUSE_CLIENT} --postgresql_connection_attempt_timeout=1 --postgresql_connection_pool_retries=1 -q \
    "CREATE DATABASE ${PG_DB} ENGINE = PostgreSQL('127.0.0.1:1', 'fake_db', 'user', 'password')"

# MySQL probes the server at construction time and tolerates the failure on ATTACH.
${CLICKHOUSE_CLIENT_QUIET} --query_id "${MYSQL_ATTACH_QUERY_ID}" -q \
    "ATTACH DATABASE ${MYSQL_DB} ENGINE = MySQL('127.0.0.1:1', 'fake_db', 'user', 'password') SETTINGS connect_timeout = 1, connection_max_tries = 1"

# A system.tables scan reaches DatabasePostgreSQL::getTablesIterator, which tolerates the failure.
${CLICKHOUSE_CLIENT_QUIET} --query_id "${PG_SCAN_QUERY_ID}" -q \
    "SELECT count() FROM system.tables WHERE database = '${PG_DB}' SETTINGS show_remote_databases_in_system_tables = 1"

# Wait for the background cleaner task's first (immediately scheduled) run to hit the connection failure.
for _ in {1..120}
do
    cleaner_rows=$(${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log; SELECT count() FROM system.text_log WHERE toUnixTimestamp64Micro(event_time_microseconds) > ${RUN_START} AND logger_name LIKE '%DatabasePostgreSQL::removeOutdatedTables%'")
    [[ "$cleaner_rows" -ge 1 ]] && break
    sleep 0.5
done

# The no-error assertions are query_id-wide on purpose: no logger on the tolerated path may write an
# Error-level line. The warning-seen ones pin each downgraded site, so the no-error checks are not vacuous.
${CLICKHOUSE_CLIENT} -q "
    SELECT 'mysql_attach_no_error', count() = 0 FROM system.text_log WHERE query_id = '${MYSQL_ATTACH_QUERY_ID}' AND level = 'Error';
    SELECT 'mysql_attach_warning_seen', count() >= 1 FROM system.text_log WHERE query_id = '${MYSQL_ATTACH_QUERY_ID}' AND logger_name = 'DatabaseMySQL' AND level = 'Warning';
    SELECT 'mysql_pool_warning_seen', count() >= 1 FROM system.text_log WHERE query_id = '${MYSQL_ATTACH_QUERY_ID}' AND logger_name = 'mysqlxx::Pool' AND level = 'Warning';
    SELECT 'mysql_failover_warning_seen', count() >= 1 FROM system.text_log WHERE query_id = '${MYSQL_ATTACH_QUERY_ID}' AND logger_name = 'Application' AND level = 'Warning' AND message LIKE 'Connection to%mysql%failed%times';
    SELECT 'pg_scan_no_error', count() = 0 FROM system.text_log WHERE query_id = '${PG_SCAN_QUERY_ID}' AND level = 'Error';
    SELECT 'pg_scan_warning_seen', count() >= 1 FROM system.text_log WHERE query_id = '${PG_SCAN_QUERY_ID}' AND logger_name LIKE '%DatabasePostgreSQL::getTablesIterator%' AND level = 'Warning';
    SELECT 'pg_pool_warning_seen', count() >= 1 FROM system.text_log WHERE query_id = '${PG_SCAN_QUERY_ID}' AND logger_name = 'PostgreSQLConnectionPool' AND level = 'Warning';
    SELECT 'pg_cleaner_no_error', count() = 0 FROM system.text_log WHERE toUnixTimestamp64Micro(event_time_microseconds) > ${RUN_START} AND (logger_name LIKE '%DatabasePostgreSQL::removeOutdatedTables%' OR logger_name = 'PostgreSQLConnectionPool') AND level = 'Error';
    SELECT 'pg_cleaner_warning_seen', count() >= 1 FROM system.text_log WHERE toUnixTimestamp64Micro(event_time_microseconds) > ${RUN_START} AND logger_name LIKE '%DatabasePostgreSQL::removeOutdatedTables%' AND level = 'Warning' AND message LIKE '%127.0.0.1:1%';
"

${CLICKHOUSE_CLIENT_QUIET} -q "DROP DATABASE ${PG_DB}"
${CLICKHOUSE_CLIENT_QUIET} -q "DROP DATABASE ${MYSQL_DB}"
