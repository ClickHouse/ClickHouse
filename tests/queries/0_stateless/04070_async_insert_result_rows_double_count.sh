#!/usr/bin/env bash
# Tags: no-fasttest
# Test that result_rows/result_bytes in HTTP X-ClickHouse-Summary are not
# double-counted for async inserts with wait_for_async_insert=1.
# result_rows in the summary must match result_rows in query_log, and must
# equal the actual number of inserted rows (not 2x).
# https://github.com/ClickHouse/ClickHouse/issues/101221

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_async_result_rows"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_async_result_rows (x String) ENGINE = MergeTree ORDER BY x"

# Insert 3 rows via HTTP with async insert, capture X-ClickHouse-Summary
query_id="ASYNC_RESULT_ROWS_$RANDOM$RANDOM"
summary=$(${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&query_id=$query_id" \
    -d "INSERT INTO test_async_result_rows VALUES ('a'), ('b'), ('c')" 2>&1 \
    | grep "X-ClickHouse-Summary" | grep -v "Access-Control-Expose-Headers" | sed 's/^.*X-ClickHouse-Summary: //')

# Extract result_rows from the summary JSON
summary_result_rows=$(echo "$summary" | python3 -c "import sys, json; print(json.load(sys.stdin)['result_rows'])")

# Also do a sync insert for comparison
sync_query_id="SYNC_RESULT_ROWS_$RANDOM$RANDOM"
sync_summary=$(${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&async_insert=0&query_id=$sync_query_id" \
    -d "INSERT INTO test_async_result_rows VALUES ('d'), ('e'), ('f')" 2>&1 \
    | grep "X-ClickHouse-Summary" | grep -v "Access-Control-Expose-Headers" | sed 's/^.*X-ClickHouse-Summary: //')

sync_summary_result_rows=$(echo "$sync_summary" | python3 -c "import sys, json; print(json.load(sys.stdin)['result_rows'])")

# Wait for query_log to be populated
for _ in $(seq 1 60);
do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    count=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.query_log
        WHERE current_database = currentDatabase()
            AND event_date >= yesterday()
            AND query_id IN ('$query_id', '$sync_query_id')
            AND type = 'QueryFinish'
        ")
    [ "$count" -ge 2 ] && break
    sleep 0.5
done

# Get result_rows from query_log for the async insert
log_result_rows=$($CLICKHOUSE_CLIENT --query "
    SELECT result_rows FROM system.query_log
    WHERE current_database = currentDatabase()
        AND query_id = '$query_id'
        AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1")

# Print results for comparison
echo "async_summary_result_rows: $summary_result_rows"
echo "async_query_log_result_rows: $log_result_rows"
echo "sync_summary_result_rows: $sync_summary_result_rows"

# The key check: async summary result_rows must equal query_log result_rows (both should be 3)
if [ "$summary_result_rows" = "$log_result_rows" ]; then
    echo "OK: summary matches query_log"
else
    echo "FAIL: summary result_rows ($summary_result_rows) != query_log result_rows ($log_result_rows)"
fi

# Async and sync should report the same result_rows for the same number of inserted rows
if [ "$summary_result_rows" = "$sync_summary_result_rows" ]; then
    echo "OK: async matches sync"
else
    echo "FAIL: async result_rows ($summary_result_rows) != sync result_rows ($sync_summary_result_rows)"
fi

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_async_result_rows"
