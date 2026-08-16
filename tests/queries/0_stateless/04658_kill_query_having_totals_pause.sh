#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-sanitizers-lsan
# Test that KILL QUERY works for TotalsHavingTransform, covering the early-return code path
# after the HAVING expression is executed with a cancellation callback.
# Uses the totals_having_transform_pause failpoint to stop the query after expression execution,
# then KILL QUERY and verify the cancellation is detected.
# no-parallel: totals_having_transform_pause is a global PAUSEABLE_ONCE failpoint, unrelated queries could consume it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="kill_query_having_totals_pause_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/kill_query_having_totals_pause_${CLICKHOUSE_DATABASE}.out"

trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT totals_having_transform_pause" 2>/dev/null' EXIT

# Enable failpoint before starting the query
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT totals_having_transform_pause"

# Start a HAVING query with totals that will pause at the failpoint.
# The client is timeout-bounded: if a regression makes the killed query never observe the
# cancellation, the test must fail here instead of hanging the whole check in `wait`.
timeout 60 ${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "
    SELECT number % 10 AS k, count()
    FROM numbers(1000000)
    GROUP BY k WITH TOTALS
    HAVING sipHash64(count()) % 2 >= 0
    FORMAT Null
    SETTINGS max_threads=1
" >"$output_file" 2>&1 &

# Wait for the failpoint to be hit (query is now blocked in transform after the HAVING expression execution).
# Bound the wait: if the query never reaches the failpoint (a plan-shape or control-flow
# regression), fail explicitly instead of hanging the whole check. Kill the stuck query (async —
# a SYNC kill of a query that never reached the pause site could hang again) and exit without
# waiting for the background job.
if ! timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT totals_having_transform_pause PAUSE"
then
    echo "FAIL: timed out waiting for the totals_having_transform_pause failpoint"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null
    exit 1
fi

# Kill the query (ASYNC) - this triggers onCancel -> cancelExecution on all functions
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

# Disable failpoint - query should see isCancelled() and call stopReading(), then early-return
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT totals_having_transform_pause"

wait

# Assert cancellation was detected, not normal completion (or a client killed by its `timeout`)
grep -qF "QUERY_WAS_CANCELLED" "$output_file" || { echo "FAIL: query was not cancelled"; cat "$output_file"; exit 1; }

echo "OK"
