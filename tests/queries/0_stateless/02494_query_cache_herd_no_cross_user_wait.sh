#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Uses global query cache and concurrent clients.
#
# With query_cache_share_between_users = 0 (default), herd coalescing must not occur across users.
# This checks per-user herd (1 Write + 4 Read) for distinct users without relying on wall-clock timing.

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

${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"

run_herd_phase "${U2}" "qrc_herd_cross_${CLICKHOUSE_DATABASE}_u2"
