#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-sanitizers-lsan, long
# Test that KILL QUERY works for FilterTransform with failpoint, covering the stopReading/early-return/cache-guard code path.
# Uses the filter_transform_pause failpoint to stop the query after expression execution,
# then KILL QUERY and verify the cancellation is detected.
# no-parallel: filter_transform_pause is a global PAUSEABLE_ONCE failpoint, unrelated queries could consume it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id="kill_query_filter_pause_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/kill_query_filter_pause_${CLICKHOUSE_DATABASE}.out"

trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT filter_transform_pause" 2>/dev/null' EXIT

# Enable failpoint before starting the query
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT filter_transform_pause"

# Start a filter query that will pause at the failpoint.
# The client is timeout-bounded: if a regression makes the killed query never observe the
# cancellation, the test must fail here instead of hanging the whole check in `wait`.
timeout 60 ${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "
    SELECT count()
    FROM numbers(100000000)
    WHERE sipHash64(number) % 2 = 1
    FORMAT Null
    SETTINGS max_block_size=10000000, max_threads=1, max_rows_to_read=0
" >"$output_file" 2>&1 &

# Wait for the failpoint to be hit (query is now blocked in doTransform after expression execution).
# The wait has no built-in timeout, so bound it: if the query never reaches the failpoint
# (a plan-shape or control-flow regression), fail explicitly instead of hanging the whole check.
# Kill the stuck query (async — a SYNC kill of a query that never reached the pause site could
# hang again) and exit without waiting for the background job.
if ! timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT filter_transform_pause PAUSE"
then
    echo "FAIL: timed out waiting for the filter_transform_pause failpoint"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null
    exit 1
fi

# Kill the query (ASYNC) - this triggers onCancel -> cancelExecution on all functions
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

# Disable failpoint - query should see isCancelled() and call stopReading(), then early-return
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT filter_transform_pause"

wait

# Assert cancellation was detected, not normal completion (or a client killed by its `timeout`)
grep -qF "QUERY_WAS_CANCELLED" "$output_file" || { echo "FAIL: query was not cancelled"; cat "$output_file"; exit 1; }

echo "OK"
