#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Prevents running test in parallel, but multiple queries for the single test still run in parallel.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Thundering herd for the Planner-level (`is_subquery = 1`) query result cache: concurrent identical subqueries must
# coalesce on one in-flight computation, just like concurrent identical top-level queries do.
#
# The outer queries differ from each other (the leading constant), so the top-level (`is_subquery = 0`) cache cannot
# deduplicate them - only the subquery herd can. Without coalescing every outer query scans the 20M rows itself; with
# coalescing exactly one does and the others read the subquery result from the cache.
# 20M rows is the largest scan allowed by the `max_rows_to_read` limit of the test configuration.

SUBQUERY="SELECT sum(number) AS x FROM numbers(20000000)"
# Subquery caching (and hence the subquery herd) only exists with the analyzer, so force it: the test would see
# no `is_subquery = 1` entries at all in a run configured with the old analyzer.
SETTINGS="enable_analyzer=1, use_query_cache=1, query_cache_for_subqueries=1, query_cache_min_query_runs=0, query_cache_min_query_duration=0"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"

for i in $(seq 1 5); do
    ${CLICKHOUSE_CLIENT} --query "SELECT ${i}, x FROM (${SUBQUERY}) SETTINGS ${SETTINGS}" \
        --query_id "qrc_subquery_herd_${CLICKHOUSE_DATABASE}_${i}" >/dev/null &
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
          AND query_id LIKE 'qrc_subquery_herd_${CLICKHOUSE_DATABASE}_%'
          AND type = 'QueryFinish'
    ")
    [ "${count}" -ge 5 ] && break
    sleep 0.5
done

# Exactly one query executes the subquery; the other four read its result from the cache.
${CLICKHOUSE_CLIENT} --query "
SELECT countIf(read_rows > 1000000), countIf(read_rows <= 1000000)
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND query_id LIKE 'qrc_subquery_herd_${CLICKHOUSE_DATABASE}_%'
  AND type = 'QueryFinish'
"

# A single Planner-level entry was written for the shared subquery.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.query_cache WHERE is_subquery = 1"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"

# A query containing the same subquery twice must not deadlock on its own herd token: the Planner takes the token while
# planning the first occurrence and releases it only after the subquery ran, so waiting for it would block the query on
# itself until `query_cache_herd_wait_timeout`. It returns promptly instead.
${CLICKHOUSE_CLIENT} --query "
SELECT a.x, b.x
FROM (SELECT sum(number) AS x FROM numbers(1000)) AS a, (SELECT sum(number) AS x FROM numbers(1000)) AS b
SETTINGS ${SETTINGS}, query_cache_herd_wait_timeout = 300
"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"
