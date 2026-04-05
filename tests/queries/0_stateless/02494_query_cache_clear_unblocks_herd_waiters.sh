#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Uses global query cache and concurrent clients.
#
# SYSTEM CLEAR QUERY CACHE must mark async-insert tokens done and notify waiters
# blocked in CacheBase::startAsyncInsert. Otherwise finishAsyncInsert no longer finds the key
# and waiters can stall until the herd timeout.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"

# First query is the herd executor; second blocks until CLEAR marks the token done or the first completes.
# sleep(10) keeps the executor busy long enough for the second session to enter startAsyncInsert.
QUERY="SELECT sleep(10) FORMAT Null SETTINGS use_query_cache=1, query_cache_min_query_duration=0, query_cache_min_query_runs=0, query_cache_nondeterministic_function_handling='save'"

${CLICKHOUSE_CLIENT} --query "${QUERY}" --query_id="qcache_herd_clear_1_${CLICKHOUSE_DATABASE}" >/dev/null 2>&1 &
PID1=$!

sleep 0.5

${CLICKHOUSE_CLIENT} --query "${QUERY}" --query_id="qcache_herd_clear_2_${CLICKHOUSE_DATABASE}" >/dev/null 2>&1 &
PID2=$!

sleep 1

${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"

# Without waking async tokens: waiter stays in startAsyncInsert until the executor's sleep ends
# and finishAsyncInsert runs (~10s more) plus its own sleep(10), well over 15s after CLEAR.
# With the fix: waiter wakes immediately after CLEAR and only runs its own sleep(10).
end=$((SECONDS + 15))
while kill -0 "${PID2}" 2>/dev/null; do
    if [ "${SECONDS}" -ge "${end}" ]; then
        echo "TIMEOUT waiting for herd waiter after CLEAR (async tokens not notified?)"
        kill "${PID1}" 2>/dev/null || true
        kill "${PID2}" 2>/dev/null || true
        exit 1
    fi
    sleep 0.2
done
wait "${PID2}"

wait "${PID1}" || true

echo "OK"
