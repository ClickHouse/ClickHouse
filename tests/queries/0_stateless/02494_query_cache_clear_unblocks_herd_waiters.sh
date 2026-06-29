#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Uses global query cache and concurrent clients.
#
# SYSTEM CLEAR QUERY CACHE must mark async-insert tokens done and notify waiters blocked in
# QueryResultCache::startAsyncInsert. The regression we guard against is HerdCoalescing::clear no longer
# notifying tokens. We assert the wake *reason*, not just eventual completion: after CLEAR, the waiter must
# re-probe the just-cleared (empty) cache and execute itself, finishing as a query result cache 'Write'.
# If clear stopped notifying tokens, the waiter would instead stay blocked until the executor finished,
# read the executor's freshly written entry, and finish as a 'Read' - failing this test.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

EXECUTOR_ID="qcache_herd_clear_exec_${CLICKHOUSE_DATABASE}"
WAITER_ID="qcache_herd_clear_wait_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"

# The default sleep cap is 3s (function_sleep_max_microseconds_per_block), so it is raised here - otherwise
# sleep(5) would throw TOO_SLOW, the executor would fail instantly, and the waiter would never actually block.
QUERY="SELECT sleep(5) FORMAT Null SETTINGS use_query_cache=1, query_cache_min_query_duration=0, query_cache_min_query_runs=0, query_cache_nondeterministic_function_handling='save', function_sleep_max_microseconds_per_block=20000000"

# First query is the herd executor; the second blocks in QueryResultCache::startAsyncInsert. sleep(5) keeps
# the executor busy long enough for the second session to start waiting before CLEAR runs.
${CLICKHOUSE_CLIENT} --query "${QUERY}" --query_id="${EXECUTOR_ID}" >/dev/null 2>&1 &
PID1=$!

sleep 2

${CLICKHOUSE_CLIENT} --query "${QUERY}" --query_id="${WAITER_ID}" >/dev/null 2>&1 &
PID2=$!

sleep 1.5

# The waiter is now blocked on the executor's token (the executor is still mid-sleep). Clearing the cache must
# wake it immediately.
${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"

wait "${PID2}"
wait "${PID1}" || true

# Wait for the waiter to be flushed to query_log.
for _ in $(seq 1 60); do
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    count=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count()
        FROM system.query_log
        WHERE event_date >= yesterday()
          AND event_time >= now() - 600
          AND current_database = currentDatabase()
          AND query_id = '${WAITER_ID}'
          AND type = 'QueryFinish'
    ")
    [ "${count}" -ge 1 ] && break
    sleep 0.5
done

# Expected 'Write': woken by CLEAR, the waiter found an empty cache and executed itself.
# A regressed clear (no notification) would yield 'Read' here.
${CLICKHOUSE_CLIENT} --query "
SELECT query_cache_usage
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND query_id = '${WAITER_ID}'
  AND type = 'QueryFinish'
"

${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"
