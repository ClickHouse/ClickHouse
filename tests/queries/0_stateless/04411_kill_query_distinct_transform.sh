#!/usr/bin/env bash
# Tags: no-fasttest, long, no-random-settings
#
# Verifies that KILL QUERY aborts a long-running SELECT DISTINCT inside the
# per-row buildFilter loop. no-random-settings: the query must keep running
# until we KILL it — random max_execution_time/max_rows_to_read interfere.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="distinct_cancel_${CLICKHOUSE_DATABASE}_$$"
err="${CLICKHOUSE_TMP}/04411_kill_distinct_err.txt"

# One 50M-row chunk forces DistinctTransform::buildFilter to spin without yielding.
$CLICKHOUSE_CLIENT --query_id "$query_id" \
    --max_block_size=50000000 \
    -q "SELECT DISTINCT number % 10000000 FROM numbers(50000000) FORMAT Null SETTINGS max_rows_to_read=0, log_queries=1" \
    > /dev/null 2>"$err" &
client_pid=$!

# Wait for elapsed > 1 to prove the PipelineExecutor is attached and the loop runs.
elapsed=0
for _ in $(seq 1 600); do
    elapsed=$(${CLICKHOUSE_CLIENT} -q "SELECT max(elapsed) FROM system.processes WHERE query_id = '$query_id'")
    if [ -n "$elapsed" ] && awk "BEGIN{exit !($elapsed > 1)}"; then break; fi
    sleep 0.1
done

if [ -z "$elapsed" ] || ! awk "BEGIN{exit !($elapsed > 1)}" 2>/dev/null; then
    echo "query did not reach the DistinctTransform loop"
    cat "$err"
    exit 1
fi

# Bound the KILL so a regression (cancel ignored) fails instead of hanging.
timeout 60 ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$query_id' SYNC FORMAT Null"
wait "$client_pid" 2>/dev/null || true

# Verify cancellation via query_log
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
cancelled=$($CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.query_log
    WHERE event_date >= yesterday()
        AND event_time >= now() - 600
        AND query_id = '$query_id'
        AND current_database = '$CLICKHOUSE_DATABASE'
        AND exception LIKE '%QUERY_WAS_CANCELLED%'")
if [[ "$cancelled" == "0" ]]; then
    exit 1
fi

echo "OK"
