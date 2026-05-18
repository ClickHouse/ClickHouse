#!/usr/bin/env bash
# Tags: no-sanitizers, no-debug
# Reason: timing assertion is flaky on slow builds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Ctrl-C in clickhouse-client sends a Cancel packet over TCP. The server has to invoke its
# interactive cancel callback to read it; while the executor is inside a long-running
# function call no progress packets flow, so the callback never fires from the executor's
# wait loop. `throwIfQueryCancelled` invokes the callback itself (throttled to
# `interactive_delay`), which is what this test verifies.
#
# The ARRAY JOIN list runs the resize once per element. Three copies make the natural
# completion time on the order of tens of seconds even on the fastest CI machines, so
# the 20s threshold below clearly distinguishes "fix in effect" from "running to
# completion." On the unpatched binary the same query takes well over a minute.

QUERY_ID="$CLICKHOUSE_TEST_UNIQUE_NAME"

${CLICKHOUSE_CLIENT} --query_id="$QUERY_ID" --max_threads 1 --query "
    SELECT sum(length(arrayResize(a, b))) FROM (
        SELECT a FROM generateRandom(
            'a Array(Nested(e1 Tuple(x Int256, y Float64, z Decimal(38, 10))))',
            1, 4, 4) LIMIT 1)
    ARRAY JOIN [-1000000000, -1000000000, -1000000000]::Array(Int32) AS b
    FORMAT Null
" >/dev/null 2>&1 &
CLIENT_PID=$!

for _ in {0..60}
do
    ${CLICKHOUSE_CLIENT} --query "SELECT count() > 0 FROM system.processes WHERE query_id = '$QUERY_ID'" | grep -qF '1' && break
    sleep 0.5
done

START_NS=$(date +%s%N)
kill -INT $CLIENT_PID
wait $CLIENT_PID
ELAPSED_MS=$(( ($(date +%s%N) - START_NS) / 1000000 ))

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "SELECT exception FROM system.query_log
    WHERE query_id = '$QUERY_ID' AND current_database = '$CLICKHOUSE_DATABASE' AND type != 'QueryStart'" \
    | grep -oF "QUERY_WAS_CANCELLED" | head -1

if (( ELAPSED_MS < 10000 ))
then
    echo "cancelled under 10s"
else
    echo "FAIL: cancellation took ${ELAPSED_MS}ms"
fi
