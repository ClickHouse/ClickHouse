#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: uses a PAUSEABLE failpoint; concurrent test instances would share the
# same global failpoint channel and interfere with each other's ENABLE/DISABLE sequence.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

DB_NAME="${CLICKHOUSE_DATABASE}_cancel_before_start"
QUERY_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_to_cancel_before_start"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT truncate_database_tables_pause" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '${QUERY_ID}' FORMAT Null" 2>/dev/null ||:
    wait 2>/dev/null ||:
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB_NAME} SYNC" 2>/dev/null ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB_NAME} SYNC"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DB_NAME} ENGINE = Atomic"

for i in $(seq 1 5); do
    $CLICKHOUSE_CLIENT -q "
        CREATE TABLE ${DB_NAME}.t${i} (x UInt64) ENGINE = MergeTree ORDER BY x;
        INSERT INTO ${DB_NAME}.t${i} SELECT number FROM numbers(100);
    "
done

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT truncate_database_tables_pause"

$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" -q "TRUNCATE TABLES FROM ${DB_NAME} LIKE '%'" 2>/dev/null &
TRUNCATE_PID=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT truncate_database_tables_pause PAUSE"
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '${QUERY_ID}' FORMAT Null"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT truncate_database_tables_pause"

wait "$TRUNCATE_PID" 2>/dev/null ||:

$CLICKHOUSE_CLIENT -nm -q "
    SYSTEM FLUSH LOGS query_log;

    SELECT
        type,
        exception_code,
        ProfileEvents['FailedQuery'] AS FailedQuery,
        ProfileEvents['FailedSelectQuery'] AS FailedSelectQuery,
        ProfileEvents['FailedInitialQuery'] AS FailedInitialQuery,
        ProfileEvents['FailedInitialSelectQuery'] AS FailedInitialSelectQuery
    FROM system.query_log
    WHERE event_date >= yesterday()
        AND event_time >= now() - 600
        AND current_database = currentDatabase()
        AND query_id = '${QUERY_ID}'
        AND is_initial_query
        AND type != 'QueryStart'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
    FORMAT Vertical;
"
