#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-sanitizers-lsan
# Test that KILL QUERY works while the `HAVING` expression is evaluated for the totals row in
# `TotalsHavingTransform::prepareTotals`. This is a separate execution site from `transform`:
# it runs after the main stream is already drained, when only the totals row is left,
# and it is reached only when the output format actually consumes totals.
# The totals_having_transform_totals_pause failpoint stops the query there, then it is killed.
# no-parallel: totals_having_transform_totals_pause is a global PAUSEABLE_ONCE failpoint,
# unrelated queries could consume it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="kill_query_totals_port_pause_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/kill_query_totals_port_pause_${CLICKHOUSE_DATABASE}.out"

trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT totals_having_transform_totals_pause" 2>/dev/null' EXIT

# Enable the failpoint before starting the query
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT totals_having_transform_totals_pause"

# `TabSeparated` prints the totals row, so `prepareTotals` evaluates `HAVING` for it.
# The client is timeout-bounded: if a regression makes the killed query never observe the
# cancellation, the test must fail here instead of hanging the whole check in `wait`.
timeout 60 ${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "
    SELECT number % 10 AS k, count()
    FROM numbers(100000)
    GROUP BY k WITH TOTALS
    HAVING sipHash64(count()) % 2 >= 0
    ORDER BY k
    FORMAT TabSeparated
    SETTINGS max_threads = 1
" >"$output_file" 2>&1 &

# Wait until the query is blocked in prepareTotals, after the HAVING expression of the totals row.
# Bound the wait: if the query never reaches the failpoint (a plan-shape or control-flow
# regression), fail explicitly instead of hanging the whole check. Kill the stuck query (async —
# a SYNC kill of a query that never reached the pause site could hang again) and exit without
# waiting for the background job.
if ! timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT totals_having_transform_totals_pause PAUSE"
then
    echo "FAIL: timed out waiting for the totals_having_transform_totals_pause failpoint"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null
    exit 1
fi

# Kill the query (ASYNC) - this triggers onCancel -> cancelExecution on all functions
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

# Release the failpoint - prepareTotals should observe the cancellation and return early
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT totals_having_transform_totals_pause"

wait

grep -qF "QUERY_WAS_CANCELLED" "$output_file" || { echo "FAIL: query was not cancelled"; cat "$output_file"; exit 1; }

echo "OK"
