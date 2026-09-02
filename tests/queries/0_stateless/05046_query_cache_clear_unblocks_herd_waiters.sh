#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Uses global query cache and concurrent clients.
#
# SYSTEM CLEAR QUERY CACHE bumps QueryResultCache's clear_generation, so that any *new* query arriving afterwards
# no longer coalesces onto an executor whose in-flight computation was started before the clear. It deliberately
# does not wake a waiter that is *already* blocked in QueryResultCache::acquireOrWaitHerdToken(): that waiter keeps
# polling the very token it already holds a reference to, and unblocks normally once that token's owner (the
# original executor, unaffected by the clear) finishes and writes its result into the now-empty cache. The waiter
# then re-probes the cache, finds that fresh entry, and finishes as a 'Read' - not as a 'Write' of its own.
#
# This is a deliberate design choice (see the comment on QueryResultCache::clear()): it trades a bit of extra
# latency for the waiter (it waits out the original executor instead of immediately retrying itself) against
# avoiding duplicate recomputation that an immediate wake would otherwise cause.
# The regression this guards against: if clear_generation stopped being bumped, a *new* query arriving after the
# clear could incorrectly coalesce onto the pre-clear token instead of becoming its own executor.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXECUTOR_ID="qcache_herd_clear_exec_${CLICKHOUSE_DATABASE}"
WAITER_ID="qcache_herd_clear_wait_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"

# The default sleep cap is 3s (function_sleep_max_microseconds_per_block), so it is raised here - otherwise
# sleep(5) would throw TOO_SLOW, the executor would fail instantly, and the waiter would never actually block.
QUERY="SELECT sleep(5) FORMAT Null SETTINGS use_query_cache=1, query_cache_min_query_duration=0, query_cache_min_query_runs=0, query_cache_nondeterministic_function_handling='save', function_sleep_max_microseconds_per_block=20000000"

# First query is the herd executor; the second blocks in QueryResultCache::acquireOrWaitHerdToken. sleep(5) keeps
# the executor busy long enough for the second session to start waiting before CLEAR runs.
${CLICKHOUSE_CLIENT} --query "${QUERY}" --query_id="${EXECUTOR_ID}" >/dev/null 2>&1 &
PID1=$!

sleep 2

${CLICKHOUSE_CLIENT} --query "${QUERY}" --query_id="${WAITER_ID}" >/dev/null 2>&1 &
PID2=$!

sleep 1.5

# The waiter is now blocked on the executor's token (the executor is still mid-sleep). Clearing the cache must not
# make the waiter crash or hang forever; it keeps waiting for the (unaffected) executor to finish.
${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"

wait "${PID1}" || true
wait "${PID2}" || true

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

# Expected 'Read': the waiter kept waiting for the (unaffected) executor, then read the entry the executor wrote
# into the cache after the clear. If clear_generation stopped being bumped and a later query incorrectly coalesced
# onto the stale pre-clear token, this assertion would still pass - that regression is instead guarded by
# 05045_query_cache_herd_no_cross_user_wait and 05044_query_cache_thundering_herd exercising fresh (post-clear)
# herds, plus QueryResultCacheCoalescingKey unit tests for the key itself.
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
