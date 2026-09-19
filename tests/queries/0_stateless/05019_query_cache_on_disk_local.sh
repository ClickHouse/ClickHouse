#!/usr/bin/env bash
# Test for the query cache on disk (setting `query_cache_on_disk_cache_name`), backed by a filesystem cache.
# The entries survive restarts, which is tested here with separate `clickhouse-local` invocations sharing one cache directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CACHE_DIR="${CLICKHOUSE_TMP}/05019_cache_${CLICKHOUSE_DATABASE}"
CONFIG_FILE="${CLICKHOUSE_TMP}/05019_config_${CLICKHOUSE_DATABASE}.yaml"
rm -rf "${CACHE_DIR}"

cat > "${CONFIG_FILE}" <<EOF
filesystem_caches:
    query_results:
        path: '${CACHE_DIR}/'
        max_size: '100M'
EOF

on_disk_cache_settings="use_query_cache = true, query_cache_on_disk_cache_name = 'query_results'"

query="SELECT number % 2 AS k, sum(number), max(toString(number * 3)), [toLowCardinality('x'), NULL] FROM numbers(1000) GROUP BY k WITH TOTALS ORDER BY k SETTINGS ${on_disk_cache_settings}, extremes = 1"
events_query="SELECT event FROM system.events WHERE event LIKE 'QueryCacheOnDisk%' AND value > 0 ORDER BY event"

echo "-- First process: computes the result and writes it to disk"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}; ${events_query};"

echo "-- Second process: serves the result from disk (the in-memory query cache of a new process is empty)"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}; ${events_query};"

echo "-- Non-deterministic results also survive the restart when served from the cache"
value1=$(${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "SELECT rand64() SETTINGS ${on_disk_cache_settings}, query_cache_nondeterministic_function_handling = 'save'")
value2=$(${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "SELECT rand64() SETTINGS ${on_disk_cache_settings}, query_cache_nondeterministic_function_handling = 'save'")
if [ "${value1}" == "${value2}" ]; then echo "same value in both processes"; else echo "different values: ${value1} ${value2}"; fi

echo "-- With reads from the query cache on disk disabled, the entry is ignored (0 hits expected)"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}, enable_reads_from_query_cache_on_disk = false; SELECT countIf(event = 'QueryCacheOnDiskHits' AND value > 0) FROM system.events;"

echo "-- A stale entry (expired TTL) is a cache miss"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "SELECT 'ttl test' SETTINGS ${on_disk_cache_settings}, query_cache_ttl = 1" > /dev/null
sleep 2
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "SELECT 'ttl test' SETTINGS ${on_disk_cache_settings}, query_cache_ttl = 1; ${events_query};"

rm -rf "${CACHE_DIR}"
rm "${CONFIG_FILE}"
