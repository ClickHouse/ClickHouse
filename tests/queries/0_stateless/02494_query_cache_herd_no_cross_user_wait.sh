#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Uses global query cache, creates users, and runs concurrent clients.
#
# With query_cache_share_between_users = 0 (default), herd coalescing must not occur across users:
# HerdCoalescingKey includes the user id/roles, so two users running the same query each get their own
# executor. We start user U1's herd first (its executor is a slow sleep that stays in-flight), then start
# user U2's herd *while U1's executor is still running* and with NO SYSTEM CLEAR QUERY CACHE in between.
# We assert 1 Write + 4 Read per user. If HerdCoalescingKey regressed to AST-only matching, U2's queries
# would coalesce on U1's still-running token; when U1 finishes they would re-probe, fail to read U1's
# user-scoped entry (query_cache_share_between_users = 0), and execute on their own - producing 5 Writes
# for U2 (0 Read) and failing this test.
#
# The slow executor's sleep is deliberately longer than the U1->U2 launch stagger so that U1's executor is
# guaranteed in-flight when U2 starts, and U1's waiters read U1's entry well before U2's executor overwrites
# the AST-keyed cache slot (the cache map key is AST-only; per-user separation is enforced on read).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

U1="${CLICKHOUSE_DATABASE}_qcache_herd_u1"
U2="${CLICKHOUSE_DATABASE}_qcache_herd_u2"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${U1}, ${U2}" 2>/dev/null || true
    ${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE" 2>/dev/null || true
}
trap cleanup EXIT

# The default sleep cap is 3s, so it is raised here - otherwise sleep(4) would throw TOO_SLOW.
QUERY="SELECT sleep(4) FORMAT Null SETTINGS use_query_cache=1, query_cache_min_query_duration=0, query_cache_min_query_runs=0, query_cache_share_between_users=0, query_cache_nondeterministic_function_handling='save', function_sleep_max_microseconds_per_block=20000000"

${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${U1}, ${U2}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${U1} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${U2} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON *.* TO ${U1}, ${U2}"

P1="qrc_herd_cross_${CLICKHOUSE_DATABASE}_u1"
P2="qrc_herd_cross_${CLICKHOUSE_DATABASE}_u2"

# Wave 1: user U1's herd (1 executor that sleeps + 4 waiters).
for i in $(seq 1 5); do
    ${CLICKHOUSE_CLIENT} --user "${U1}" --query "${QUERY}" --query_id "${P1}_${i}" >/dev/null 2>&1 &
done

# Let U1's executor get in-flight, then start U2's herd while it is still running.
sleep 2

# Wave 2: user U2's herd, concurrently with U1's still-running executor.
for i in $(seq 1 5); do
    ${CLICKHOUSE_CLIENT} --user "${U2}" --query "${QUERY}" --query_id "${P2}_${i}" >/dev/null 2>&1 &
done

wait

# Wait until all 10 queries are in query_log before reading it.
for _ in $(seq 1 60); do
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    count=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count()
        FROM system.query_log
        WHERE event_date >= yesterday()
          AND event_time >= now() - 600
          AND current_database = currentDatabase()
          AND (query_id LIKE '${P1}_%' OR query_id LIKE '${P2}_%')
          AND type = 'QueryFinish'
    ")
    [ "${count}" -ge 10 ] && break
    sleep 0.5
done

report_user()
{
    local id_prefix="$1"

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

report_user "${P1}"
report_user "${P2}"
