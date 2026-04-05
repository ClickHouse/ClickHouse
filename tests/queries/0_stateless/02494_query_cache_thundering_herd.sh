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

# Test: WITH TOTALS results are preserved correctly for both the executor thread and waiters.
# All 5 concurrent runs of the same WITH TOTALS query should produce identical output that
# matches a fresh (non-cached) execution, including the totals row.

TOTALS_QUERY="SELECT number % 2 AS k, count() FROM numbers(6) GROUP BY k WITH TOTALS ORDER BY k \
    SETTINGS use_query_cache=1, query_cache_min_query_runs=0, query_cache_min_query_duration=0"

${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"

expected_totals=$(${CLICKHOUSE_CLIENT} --query \
    "SELECT number % 2 AS k, count() FROM numbers(6) GROUP BY k WITH TOTALS ORDER BY k")

tmpdir_totals=$(mktemp -d)
for i in $(seq 1 5); do
    ${CLICKHOUSE_CLIENT} --query "${TOTALS_QUERY}" > "${tmpdir_totals}/result_${i}" &
done
wait

all_match=1
for i in $(seq 1 5); do
    if [ "$(cat "${tmpdir_totals}/result_${i}")" != "${expected_totals}" ]; then
        all_match=0
        break
    fi
done
echo "${all_match}"

rm -rf "${tmpdir_totals}"
${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"

# Test: extremes are preserved correctly for both the executor thread and waiters.
# All 5 concurrent runs should produce identical output that matches a fresh execution,
# including the extremes rows (min/max).

EXTREMES_QUERY="SELECT number FROM numbers(5) \
    SETTINGS extremes=1, use_query_cache=1, query_cache_min_query_runs=0, query_cache_min_query_duration=0"

${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"

expected_extremes=$(${CLICKHOUSE_CLIENT} --query "SELECT number FROM numbers(5) SETTINGS extremes=1")

tmpdir_extremes=$(mktemp -d)
for i in $(seq 1 5); do
    ${CLICKHOUSE_CLIENT} --query "${EXTREMES_QUERY}" > "${tmpdir_extremes}/result_${i}" &
done
wait

all_match=1
for i in $(seq 1 5); do
    if [ "$(cat "${tmpdir_extremes}/result_${i}")" != "${expected_extremes}" ]; then
        all_match=0
        break
    fi
done
echo "${all_match}"

rm -rf "${tmpdir_extremes}"
${CLICKHOUSE_CLIENT} --query "SYSTEM CLEAR QUERY CACHE"
