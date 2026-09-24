#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=helpers/native_cancel.sh
. "$CUR_DIR"/helpers/native_cancel.sh

set -euo pipefail

QUERY_ID="${CLICKHOUSE_DATABASE}_native_cancel_explain_analyze_${RANDOM}_$$"
CLIENT_OUT="${CLICKHOUSE_TMP}/native_cancel_explain_analyze.out"
CLIENT_ERR="${CLICKHOUSE_TMP}/native_cancel_explain_analyze.err"
CLIENT_PID=""

cleanup()
{
    native_cancel_cleanup_client
    rm -f "$CLIENT_OUT" "$CLIENT_ERR"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" \
    --partial_result_on_first_cancel=1 \
    --max_threads=1 --max_block_size=1000 --interactive_delay=1000 \
    --max_rows_to_read=0 --max_bytes_to_read=0 --max_execution_time=0 \
    --enable_parallel_replicas=0 \
    --log_queries=1 --log_queries_probability=1 --log_queries_min_query_duration_ms=0 \
    --query "EXPLAIN ANALYZE SELECT sum(number) FROM numbers(1000000000)
        WHERE sleepEachRow(0.00001) = 0" > "$CLIENT_OUT" 2> "$CLIENT_ERR" &
CLIENT_PID=$!

if ! native_cancel_wait_for_process \
    "$QUERY_ID" \
    "read_rows >= 1000" \
    "EXPLAIN ANALYZE did not start reading" \
    "$CLIENT_ERR"; then
    exit 1
fi

kill -INT "$CLIENT_PID"
wait "$CLIENT_PID" || true
CLIENT_PID=""

if [[ -s "$CLIENT_OUT" ]]; then
    echo "Cancelled EXPLAIN ANALYZE returned partial output"
    cat "$CLIENT_OUT"
    exit 1
fi

if ! native_cancel_wait_for_query_log "$QUERY_ID" \
    "count() = 1
        AND countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND exception_code = 735) = 1
        AND countIf(type = 'QueryFinish') = 0"; then
    echo "EXPLAIN ANALYZE did not report full cancellation"
    cat "$CLIENT_ERR"
    exit 1
fi

echo "cancelled without partial explain"

normal_output=$($CLICKHOUSE_CLIENT --query "EXPLAIN ANALYZE SELECT sum(number) FROM numbers(10)")
if ! grep -q "Query summary:" <<< "$normal_output"; then
    echo "Normal EXPLAIN ANALYZE did not return a report"
    exit 1
fi

echo "normal explain returned"
