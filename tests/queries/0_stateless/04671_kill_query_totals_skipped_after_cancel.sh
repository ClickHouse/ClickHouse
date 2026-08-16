#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-sanitizers-lsan
# Test that a `WITH TOTALS` query killed just before the totals-port phase does no totals work at
# all: `TotalsHavingTransform::prepareTotals` must return immediately instead of merging overflow
# aggregates, finalizing aggregate states, and evaluating `HAVING` for the totals row.
# The query is paused at the very beginning of `prepareTotals` and killed there; only that
# failpoint is then released, while `totals_having_transform_totals_pause` (which sits after the
# `HAVING` expression of the totals row) stays enabled. If the totals work still ran, the query
# would block on it and never finish.
# no-parallel: the failpoints are global `PAUSEABLE_ONCE`, unrelated queries could consume them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="kill_query_totals_skipped_${CLICKHOUSE_DATABASE}_$RANDOM"
output_file="${CLICKHOUSE_TMP}/kill_query_totals_skipped_${CLICKHOUSE_DATABASE}.out"

disable_failpoints() {
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT totals_having_transform_totals_start_pause" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT totals_having_transform_totals_pause" 2>/dev/null
}

trap disable_failpoints EXIT

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT totals_having_transform_totals_start_pause"
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT totals_having_transform_totals_pause"

# `TabSeparated` prints the totals row, so the totals port is consumed and `prepareTotals` runs.
# The client is timeout-bounded as a backstop for the `wait` calls below: the poll loop already
# fails the test when the cancelled query keeps doing the totals work, but the client must not be
# able to outlive it and hold the whole check.
timeout 60 ${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "
    SELECT number % 10 AS k, count()
    FROM numbers(100000)
    GROUP BY k WITH TOTALS
    HAVING sipHash64(count()) % 2 >= 0
    ORDER BY k
    FORMAT TabSeparated
    SETTINGS max_threads = 1
" >"$output_file" 2>&1 &
query_pid=$!

# Wait until the query is blocked at the entry of `prepareTotals`, before any totals work.
# Bound the wait: if the query never reaches the failpoint (a plan-shape or control-flow
# regression), fail explicitly instead of hanging the whole check. Kill the stuck query (async —
# a SYNC kill of a query that never reached the pause site could hang again) and exit without
# waiting for the background job; the EXIT trap disables the remaining failpoints.
if ! timeout 60 ${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT totals_having_transform_totals_start_pause PAUSE"
then
    echo "FAIL: timed out waiting for the totals_having_transform_totals_start_pause failpoint"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null
    exit 1
fi

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_wait_end_of_query=0" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

# Release only the entry failpoint; the one after the totals `HAVING` expression stays enabled
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT totals_having_transform_totals_start_pause"

# The query must finish without evaluating the totals row
for _ in {1..300}
do
    kill -0 "$query_pid" 2>/dev/null || break
    sleep 0.1
done

if kill -0 "$query_pid" 2>/dev/null
then
    echo "FAIL: the cancelled query is still doing the totals work"
    disable_failpoints
    wait
    exit 1
fi

wait

grep -qF "QUERY_WAS_CANCELLED" "$output_file" || { echo "FAIL: query was not cancelled"; cat "$output_file"; exit 1; }

echo "OK"
