#!/usr/bin/env bash
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_to_cancel"

$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" -q "
    SELECT sleepEachRow(0.5), rand64() FROM numbers(1000)
    FORMAT Null
    SETTINGS max_block_size = 1,
             function_sleep_max_microseconds_per_block = 1000000000
" &
CLIENT_PID=$!

wait_for_query_to_start "$QUERY_ID"

kill -INT "$CLIENT_PID"
wait "$CLIENT_PID"

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
        AND type != 'QueryStart'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
    FORMAT Vertical;
"
