#!/usr/bin/env bash
# Tags: no-fasttest
# Test that async insert progress stats are properly reported
# to clients and recorded in query_log for both TCP and HTTP protocols.
# https://github.com/ClickHouse/ClickHouse/issues/99758

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_async_insert_progress"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_async_insert_progress (x String) ENGINE = MergeTree ORDER BY x"

# Test 1: TCP protocol (clickhouse-client uses TCP)
query_tcp_values_id="ASYNC_INSERT_TCP_VALUES_$RANDOM$RANDOM"
${CLICKHOUSE_CLIENT} --progress err --query_id="$query_tcp_values_id" --async_insert 1 --wait_for_async_insert 1 --query \
    "INSERT INTO test_async_insert_progress VALUES ('one'), ('two'), ('three')" 2>&1 \
    | grep "Progress:" |  sed 's/.*Progress: \([0-9.]*\) rows, \([0-9.]* [A-Za-z]*\).*/TCP client values: Rows: \1, Bytes: \2/'

query_tcp_data_id="ASYNC_INSERT_TCP_DATA_$RANDOM$RANDOM"
echo -e "one\n two\n three" | ${CLICKHOUSE_CLIENT} --progress err --query_id="$query_tcp_data_id" --async_insert 1 --wait_for_async_insert 1 --query \
    "INSERT INTO test_async_insert_progress FORMAT TSV" 2>&1 \
    | grep "Progress:" |  sed 's/.*Progress: \([0-9.]*\) rows, \([0-9.]* [A-Za-z]*\).*/TCP client data: Rows: \1, Bytes: \2/'

# Test 2: HTTP protocol
query_http_id="ASYNC_INSERT_HTTP_$RANDOM$RANDOM"
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&query_id=$query_http_id" \
    -d "INSERT INTO test_async_insert_progress VALUES ('four'), ('five'), ('six')" 2>&1 \
    | grep "X-ClickHouse-Summary" | grep -v "Access-Control-Expose-Headers" | sed 's/,\"elapsed_ns[^}]*//' | sed 's/,\"memory_usage[^}]*//'

for _ in $(seq 1 60);
do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    count=$($CLICKHOUSE_CLIENT -q "
        SELECT count(DISTINCT query_id) FROM system.query_log
        WHERE
            current_database = currentDatabase()
            AND event_date >= yesterday()
            AND query_id = '$query_http_id'
        ")
    [ "$count" -ge 1 ] && break
    sleep 0.5
done

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} --query "
    SELECT 'TCP data query log:', read_rows, read_bytes, written_rows, written_bytes, result_rows, result_bytes, interface
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND query_id = '$query_tcp_data_id'
        AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC
    LIMIT 1"

${CLICKHOUSE_CLIENT} --query "
    SELECT 'TCP values query log:', read_rows, read_bytes, written_rows, written_bytes, result_rows, result_bytes, interface
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND query_id = '$query_tcp_values_id'
        AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC
    LIMIT 1"

${CLICKHOUSE_CLIENT} --query "
    SELECT 'HTTP query log:', read_rows, read_bytes, written_rows, written_bytes, result_rows, result_bytes, interface
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND query_id = '$query_http_id'
        AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC
    LIMIT 1"

# Verify data actually got written
${CLICKHOUSE_CLIENT} --query "SELECT 'total_rows', count() FROM test_async_insert_progress"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_async_insert_progress"
