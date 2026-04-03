#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Prevents running test in parallel, but multiple queries for the single test still run in parallel.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY="SELECT sum(number) FROM numbers(20000000) SETTINGS use_query_cache=1, query_cache_min_query_duration=0, query_cache_min_query_runs=0"

${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"

for i in $(seq 1 5); do
    ${CLICKHOUSE_CLIENT} --query "${QUERY}" --query_id "qrc_thundering_herd_${CLICKHOUSE_DATABASE}_${i}" >/dev/null &
done
wait

# Wait for all 5 queries to appear in query_log before reading it.
for _ in $(seq 1 60); do
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    count=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count()
        FROM system.query_log
        WHERE event_date >= yesterday()
          AND event_time >= now() - 600
          AND current_database = currentDatabase()
          AND query_id LIKE 'qrc_thundering_herd_${CLICKHOUSE_DATABASE}_%'
          AND type = 'QueryFinish'
    ")
    [ "${count}" -ge 5 ] && break
    sleep 0.5
done

# One query should write to the cache.
${CLICKHOUSE_CLIENT} --query "
SELECT count()
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND query_id LIKE 'qrc_thundering_herd_${CLICKHOUSE_DATABASE}_%'
  AND type = 'QueryFinish'
  AND query_cache_usage = 'Write'
"

# Remaining 4 queries should read from the cache.
${CLICKHOUSE_CLIENT} --query "
SELECT count()
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND query_id LIKE 'qrc_thundering_herd_${CLICKHOUSE_DATABASE}_%'
  AND type = 'QueryFinish'
  AND query_cache_usage = 'Read'
"

${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"
