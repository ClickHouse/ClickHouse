#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Prevents running test in parallel, but multiple queries for the single test still run in parallel.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Verifies that when the herd "executor" is killed mid-flight while the overall
# query_cache_herd_wait_timeout budget still has plenty of time left, the waiters that lose the
# takeover race rejoin the wait on the new executor's token instead of falling through to
# independent, uncoalesced execution ("mini thundering herd"). See acquireOrWaitHerdToken() /
# tryBecomeHerdExecutor() in QueryResultCache.cpp and their callers in executeQuery.cpp.
#
# The overall timeout (20s) is deliberately much larger than the time the kill+takeover round
# actually takes (a couple of seconds), so there is ample budget left for the retry loop to matter.
# The executor sleeps long enough (10s) that we can reliably KILL it mid-flight.

NUM_WAITERS=6
EXECUTOR_QUERY_ID="05049_herd_kill_${CLICKHOUSE_DATABASE}_executor"

QUERY="SELECT sleepEachRow(10) FROM numbers(1) \
    SETTINGS use_query_cache = 1, \
             query_cache_min_query_duration = 0, \
             query_cache_min_query_runs = 0, \
             query_cache_herd_wait_timeout = 20, \
             query_cache_nondeterministic_function_handling = 'save', \
             function_sleep_max_microseconds_per_block = 20000000"

${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"

${CLICKHOUSE_CLIENT} --query "${QUERY}" --query_id "${EXECUTOR_QUERY_ID}" >/dev/null 2>&1 &
executor_pid=$!

# Give the executor time to actually become the herd executor before the waiters arrive.
sleep 1

waiter_pids=()
for i in $(seq 1 "${NUM_WAITERS}"); do
    ${CLICKHOUSE_CLIENT} --query "${QUERY}" --query_id "05049_herd_kill_${CLICKHOUSE_DATABASE}_waiter_${i}" >/dev/null 2>&1 &
    waiter_pids+=($!)
done

# Let the waiters settle into the wait on the executor's token before killing it.
sleep 1

${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '${EXECUTOR_QUERY_ID}' SYNC" >/dev/null

wait "${executor_pid}" 2>/dev/null
for pid in "${waiter_pids[@]}"; do
    wait "${pid}"
done

# Wait for all waiter queries to appear in query_log before reading it.
for _ in $(seq 1 60); do
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    count=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count()
        FROM system.query_log
        WHERE event_date >= yesterday()
          AND event_time >= now() - 600
          AND current_database = currentDatabase()
          AND query_id LIKE '05049_herd_kill_${CLICKHOUSE_DATABASE}_waiter_%'
          AND type = 'QueryFinish'
    ")
    [ "${count}" -ge "${NUM_WAITERS}" ] && break
    sleep 0.5
done

# Exactly one waiter should have taken over as the new executor and written to the cache.
${CLICKHOUSE_CLIENT} --query "
SELECT count()
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND query_id LIKE '05049_herd_kill_${CLICKHOUSE_DATABASE}_waiter_%'
  AND type = 'QueryFinish'
  AND query_cache_usage = 'Write'
"

# The remaining waiters should have coalesced onto the takeover winner and read from the cache.
${CLICKHOUSE_CLIENT} --query "
SELECT count()
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND query_id LIKE '05049_herd_kill_${CLICKHOUSE_DATABASE}_waiter_%'
  AND type = 'QueryFinish'
  AND query_cache_usage = 'Read'
"

${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"
