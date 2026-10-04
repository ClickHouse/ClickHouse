#!/usr/bin/env bash
# The per-user limits `query_cache_max_size_in_bytes` and `query_cache_max_entries` apply to the in-memory query cache only.
# The query cache on disk (setting `query_cache_on_disk_cache_name`) is bounded by the underlying filesystem cache instead,
# which is documented behaviour, so writes and reads must keep working under limits that would reject everything in memory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CACHE_DIR="${CLICKHOUSE_TMP}/05055_cache_${CLICKHOUSE_DATABASE}"
CONFIG_FILE="${CLICKHOUSE_TMP}/05055_config_${CLICKHOUSE_DATABASE}.yaml"
rm -rf "${CACHE_DIR}"

cat > "${CONFIG_FILE}" <<EOF
filesystem_caches:
    query_results:
        path: '${CACHE_DIR}/'
        max_size: '100M'
EOF

# Only the on-disk backend is exercised: the in-memory cache is disabled explicitly.
settings="use_query_cache = true, query_cache_on_disk_cache_name = 'query_results', enable_writes_to_query_cache = false, enable_reads_from_query_cache = false, query_cache_max_size_in_bytes = 1, query_cache_max_entries = 1"

events_query="SELECT event FROM system.events WHERE event LIKE 'QueryCacheOnDisk%' AND value > 0 ORDER BY event"

echo "-- Two distinct results are written to disk although only one tiny entry would be allowed in memory"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    SELECT sum(number) FROM numbers(1000) SETTINGS ${settings};
    SELECT sum(number) FROM numbers(2000) SETTINGS ${settings};
    ${events_query};"

echo "-- Both results are served from disk"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    SELECT sum(number) FROM numbers(1000) SETTINGS ${settings};
    SELECT sum(number) FROM numbers(2000) SETTINGS ${settings};
    SELECT value FROM system.events WHERE event = 'QueryCacheOnDiskHits';"

rm -rf "${CACHE_DIR}"
rm "${CONFIG_FILE}"
