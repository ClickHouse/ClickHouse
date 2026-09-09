#!/usr/bin/env bash
# `QueryCacheHits` / `QueryCacheMisses` / `QueryCacheAgeSeconds` describe the query cache as a whole, also when the query
# cache on disk (setting `query_cache_on_disk_cache_name`) is enabled and both backends are probed: a miss in memory
# followed by a hit on disk is one hit and no miss, and a lookup that consults only the disk still records a miss.
# `QueryCacheOnDiskHits` / `QueryCacheOnDiskMisses` are the breakdown of the on-disk backend alone.
# `clickhouse-local` is used because it disables the in-memory query cache (its entry size limits are 0), which makes
# every lookup in memory a miss, and because each process starts with all profile events at zero.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CACHE_DIR="${CLICKHOUSE_TMP}/05154_cache_${CLICKHOUSE_DATABASE}"
CONFIG_FILE="${CLICKHOUSE_TMP}/05154_config_${CLICKHOUSE_DATABASE}.yaml"
rm -rf "${CACHE_DIR}"

cat > "${CONFIG_FILE}" <<EOF
filesystem_caches:
    query_results:
        path: '${CACHE_DIR}/'
        max_size: '100M'
EOF

settings="use_query_cache = true, query_cache_on_disk_cache_name = 'query_results'"
events_query="
    SELECT event, value FROM system.events
        WHERE event IN ('QueryCacheHits', 'QueryCacheMisses', 'QueryCacheOnDiskHits', 'QueryCacheOnDiskMisses')
        ORDER BY event;
    SELECT 'QueryCacheAgeSeconds >= 1', sum(value) >= 1 FROM system.events WHERE event = 'QueryCacheAgeSeconds';"

echo "-- A miss in both backends is one QueryCacheMisses and no age"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" \
    --query "SELECT sum(number) FROM numbers(1000) SETTINGS ${settings}; ${events_query}"

# The age of the entry is reported in whole seconds, so the hit below has to happen at least one second after the write.
sleep 1.1

echo "-- A miss in memory followed by a hit on disk is one QueryCacheHits, no miss, and the age of the entry"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" \
    --query "SELECT sum(number) FROM numbers(1000) SETTINGS ${settings}; ${events_query}"

echo "-- A lookup that consults only the disk still records the overall miss"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" \
    --query "SELECT sum(number) FROM numbers(1001) SETTINGS ${settings}, enable_reads_from_query_cache = 0; ${events_query}"

rm -rf "${CACHE_DIR}"
rm "${CONFIG_FILE}"
