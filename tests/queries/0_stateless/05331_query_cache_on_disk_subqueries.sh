#!/usr/bin/env bash
# Test that the results of subqueries are cached in the query cache on disk (setting `query_cache_on_disk_cache_name`).
# `clickhouse-local` has no in-memory query cache, so a hit in a second process proves that the subquery result is served from disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CACHE_DIR="${CLICKHOUSE_TMP}/05331_cache_${CLICKHOUSE_DATABASE}"
CONFIG_FILE="${CLICKHOUSE_TMP}/05331_config_${CLICKHOUSE_DATABASE}.yaml"
rm -rf "${CACHE_DIR}"

cat > "${CONFIG_FILE}" <<EOF
filesystem_caches:
    query_results:
        path: '${CACHE_DIR}/'
        max_size: '100M'
EOF

events_query="SELECT event, value FROM system.events WHERE event IN ('QueryCacheOnDiskHits', 'QueryCacheOnDiskMisses') ORDER BY event"
query_settings="query_cache_on_disk_cache_name = 'query_results', query_cache_nondeterministic_function_handling = 'save'"

# The subqueries are non-deterministic, so the same value in both processes proves that the second one reads the cached subquery result.
# The outer queries differ between the processes, so the second process cannot be served by an entry of the outer query.

echo "-- Per-subquery setting: only the subquery is cached"
subquery="SELECT rand64() AS r, number FROM numbers(10) SETTINGS use_query_cache = true, ${query_settings}"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "SELECT r FROM (${subquery}) WHERE number = 7 SETTINGS ${query_settings} FORMAT Null; ${events_query};"
value1=$(${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "SELECT r FROM (${subquery}) WHERE number = 7 SETTINGS ${query_settings}")
value2=$(${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "SELECT r FROM (${subquery}) WHERE number IN (7) SETTINGS ${query_settings}; ${events_query};")
echo "${value2}" | tail -n +2
if [ "${value1}" == "$(echo "${value2}" | head -n 1)" ]; then echo "same value in both processes"; else echo "different values: ${value1} ${value2}"; fi

echo "-- Setting query_cache_for_subqueries"
subquery="SELECT rand64() AS r, number FROM numbers(20)"
outer_settings="use_query_cache = true, query_cache_for_subqueries = true, ${query_settings}"
value1=$(${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "SELECT r FROM (${subquery}) WHERE number = 7 SETTINGS ${outer_settings}; ${events_query};")
value2=$(${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "SELECT r FROM (${subquery}) WHERE number IN (7) SETTINGS ${outer_settings}; ${events_query};")
echo "${value1}" | tail -n +2
echo "${value2}" | tail -n +2
if [ "$(echo "${value1}" | head -n 1)" == "$(echo "${value2}" | head -n 1)" ]; then echo "same value in both processes"; else echo "different values: ${value1} ${value2}"; fi

rm -rf "${CACHE_DIR}"
rm "${CONFIG_FILE}"
