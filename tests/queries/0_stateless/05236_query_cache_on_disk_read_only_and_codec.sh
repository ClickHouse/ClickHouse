#!/usr/bin/env bash
# Setting `enable_writes_to_query_cache_on_disk` and setting `query_cache_on_disk_codec` of the query cache on disk
# (setting `query_cache_on_disk_cache_name`). Tested with separate `clickhouse-local` invocations sharing one cache directory, so that
# the profile events of each step start from zero.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CACHE_DIR="${CLICKHOUSE_TMP}/05236_cache_${CLICKHOUSE_DATABASE}"
CONFIG_FILE="${CLICKHOUSE_TMP}/05236_config_${CLICKHOUSE_DATABASE}.yaml"
rm -rf "${CACHE_DIR}"

cat > "${CONFIG_FILE}" <<EOF_CONFIG
filesystem_caches:
    query_results:
        path: '${CACHE_DIR}/'
        max_size: '100M'
EOF_CONFIG

settings="use_query_cache = true, query_cache_on_disk_cache_name = 'query_results'"
query="SELECT number, toString(number) FROM numbers(1000) FORMAT Null SETTINGS ${settings}"
events_query="SELECT event FROM system.events WHERE event LIKE 'QueryCacheOnDisk%' AND value > 0 ORDER BY event"
written_bytes_query="SELECT value FROM system.events WHERE event = 'QueryCacheOnDiskWrittenBytes'"

echo "-- Reads only: nothing is written, so the repeated query is a miss as well"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    ${query}, enable_writes_to_query_cache_on_disk = false;
    ${query}, enable_writes_to_query_cache_on_disk = false;
    ${events_query};"

echo "-- Neither reads nor writes: the query cache on disk is not consulted at all"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    ${query}, enable_writes_to_query_cache_on_disk = false, enable_reads_from_query_cache_on_disk = false;
    ${events_query};"

echo "-- Written with codec LZ4"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    ${query}, query_cache_on_disk_codec = 'LZ4';
    ${events_query};"

echo "-- Read with a different codec setting: the compression frames are self-describing"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    ${query}, query_cache_on_disk_codec = 'NONE';
    ${events_query};"

echo "-- Reads only: the entry is served, and the codec setting is not validated because it is not needed"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    ${query}, enable_writes_to_query_cache_on_disk = false, query_cache_on_disk_codec = 'NO_SUCH_CODEC';
    ${events_query};"

echo "-- An invalid codec fails the query when writes are enabled"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}, query_cache_on_disk_codec = 'NO_SUCH_CODEC'" 2>&1 | grep -o "UNKNOWN_CODEC"

echo "-- The codec setting takes effect: the same result is bigger uncompressed than compressed"
bytes_none=$(${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    SELECT number FROM numbers(10000) FORMAT Null SETTINGS ${settings}, query_cache_on_disk_codec = 'NONE', query_cache_tag = 'none';
    ${written_bytes_query};")
bytes_zstd=$(${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    SELECT number FROM numbers(10000) FORMAT Null SETTINGS ${settings}, query_cache_on_disk_codec = 'ZSTD(3)', query_cache_tag = 'zstd';
    ${written_bytes_query};")
if [ "${bytes_none}" -gt 80000 ] && [ "${bytes_zstd}" -lt "${bytes_none}" ]; then echo "ok"; else echo "unexpected sizes: ${bytes_none} ${bytes_zstd}"; fi

rm -rf "${CACHE_DIR}"
rm "${CONFIG_FILE}"
