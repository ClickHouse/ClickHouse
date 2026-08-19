#!/usr/bin/env bash
# The maximum entry size of the query cache (server setting `query_cache.max_entry_size_in_bytes`) also applies to the query cache on
# disk, in particular when writes to the in-memory query cache are disabled and the on-disk cache is the only backend.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CACHE_DIR="${CLICKHOUSE_TMP}/05021_cache_${CLICKHOUSE_DATABASE}"
CONFIG_FILE="${CLICKHOUSE_TMP}/05021_config_${CLICKHOUSE_DATABASE}.yaml"
rm -rf "${CACHE_DIR}"

# The result below is far larger than one byte, so it must not be cached at all.
cat > "${CONFIG_FILE}" <<EOF
query_cache:
    max_entry_size_in_bytes: 1
filesystem_caches:
    query_results:
        path: '${CACHE_DIR}/'
        max_size: '100M'
EOF

on_disk_cache_settings="use_query_cache = true, query_cache_on_disk_cache_name = 'query_results'"
query="SELECT number FROM numbers(1000) SETTINGS ${on_disk_cache_settings}, enable_writes_to_query_cache = false"
written_query="SELECT sum(value) FROM system.events WHERE event = 'QueryCacheOnDiskWrittenBytes'"
hits_query="SELECT sum(value) FROM system.events WHERE event = 'QueryCacheOnDiskHits'"

echo "-- An oversized query result is not written to disk (0 bytes written expected)"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query} FORMAT Null; ${written_query};"

echo "-- Nothing was stored, so a second process cannot serve the result from disk (0 hits expected)"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query} FORMAT Null; ${hits_query};"

echo "-- A result within the limit is written to disk and served from it in a second process (1 hit expected)"
small_query="SELECT number FROM numbers(10) SETTINGS ${on_disk_cache_settings}, enable_writes_to_query_cache = false"
cat > "${CONFIG_FILE}" <<EOF
query_cache:
    max_entry_size_in_bytes: 1073741824
filesystem_caches:
    query_results:
        path: '${CACHE_DIR}/'
        max_size: '100M'
EOF
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${small_query} FORMAT Null;"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${small_query} FORMAT Null; ${hits_query};"

rm -rf "${CACHE_DIR}"
rm "${CONFIG_FILE}"
