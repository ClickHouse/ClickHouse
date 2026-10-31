#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Uses global query cache, creates users, and runs concurrent clients.
#
# With `query_cache_share_between_users = 0` (default), herd coalescing must not occur across users:
# `QueryResultCache::CoalescingKey` includes the user id and roles, so two users running the same query each get
# their own herd executor instead of one waiting on the other's in-flight computation.
#
# This test verifies per-user herd coalescing (1 `Write` + 4 `Read`) independently for two distinct users.
# It deliberately does NOT run the two users' herds concurrently against the same cache slot, because such a
# test cannot observe the cross-user property through `query_cache_usage` counts:
#
#   The cache map key is AST-only (`QueryResultCache::Key::operator==` ignores the user; per-user separation is
#   enforced only on read, in `QueryResultCacheReader`). So both users contend for the *same* cache slot, and
#   whichever executor inserts first owns it. The other user cannot read that entry
#   (`query_cache_share_between_users = 0`) and its own `finalizeWrite` is skipped because the slot already holds
#   a non-stale entry - so its waiters miss on re-probe and re-execute, yielding N `Write` / 0 `Read` for the
#   losing user. This `1/4/N/0` outcome is produced identically whether `CoalescingKey` is correctly user-scoped
#   or regressed to AST-only, so a concurrent cross-user count assertion (e.g. `1/4/1/4` or `2 Write` / `8 Read`
#   overall) is unachievable and would not distinguish the regression it targets. The cross-user non-coalescing
#   property is instead a property of `CoalescingKey` itself (same AST + different user id/roles => distinct key)
#   and is best guarded by a focused unit test on that key rather than by a coalescing count here.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

U1="${CLICKHOUSE_DATABASE}_qcache_herd_u1"
U2="${CLICKHOUSE_DATABASE}_qcache_herd_u2"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${U1}, ${U2}" 2>/dev/null || true
    ${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE" 2>/dev/null || true
}
trap cleanup EXIT

# A query slow enough that 5 concurrent runs reliably overlap (the executor is still running when the waiters
# arrive), so exactly one becomes the herd executor and the other four coalesce on it. This mirrors the
# single-user herd pattern in 05044_query_cache_thundering_herd.
QUERY="SELECT sum(number) FROM numbers(20000000) SETTINGS use_query_cache=1, query_cache_min_query_duration=0, query_cache_min_query_runs=0, query_cache_share_between_users=0"

${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${U1}, ${U2}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${U1} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${U2} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON *.* TO ${U1}, ${U2}"

run_herd_phase()
{
    local user="$1"
    local id_prefix="$2"

    for i in $(seq 1 5); do
        ${CLICKHOUSE_CLIENT} --user "${user}" --query "${QUERY}" --query_id "${id_prefix}_${i}" >/dev/null &
    done
    wait

    for _ in $(seq 1 60); do
        ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
        count=$(${CLICKHOUSE_CLIENT} --query "
            SELECT count()
            FROM system.query_log
            WHERE event_date >= yesterday()
              AND event_time >= now() - 600
              AND current_database = currentDatabase()
              AND query_id LIKE '${id_prefix}_%'
              AND type = 'QueryFinish'
        ")
        [ "${count}" -ge 5 ] && break
        sleep 0.5
    done

    # One query per user writes to the cache.
    ${CLICKHOUSE_CLIENT} --query "
SELECT count()
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND query_id LIKE '${id_prefix}_%'
  AND type = 'QueryFinish'
  AND query_cache_usage = 'Write'
"

    # The other four queries read from that user's own cache entry.
    ${CLICKHOUSE_CLIENT} --query "
SELECT count()
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND query_id LIKE '${id_prefix}_%'
  AND type = 'QueryFinish'
  AND query_cache_usage = 'Read'
"
}

run_herd_phase "${U1}" "qrc_herd_cross_${CLICKHOUSE_DATABASE}_u1"

# Clear between users so each herd runs against an empty slot and deterministically wins it (1 Write + 4 Read),
# instead of contending for the single AST-keyed slot (see the header comment).
${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"

run_herd_phase "${U2}" "qrc_herd_cross_${CLICKHOUSE_DATABASE}_u2"
