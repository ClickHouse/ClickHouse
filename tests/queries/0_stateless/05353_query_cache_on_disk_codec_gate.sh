#!/usr/bin/env bash
# Tags: no-fasttest
# Setting `query_cache_on_disk_codec` compresses a raw `Native` byte stream without a column type, so it is validated
# like the other untyped codec settings: an experimental or beta codec needs its `enable_<family>_codec` setting,
# and a codec that requires the column type to compress (`ALP`, `PCO`) is rejected up front.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CACHE_DIR="${CLICKHOUSE_TMP}/05353_cache_${CLICKHOUSE_DATABASE}"
CONFIG_FILE="${CLICKHOUSE_TMP}/05353_config_${CLICKHOUSE_DATABASE}.yaml"
rm -rf "${CACHE_DIR}"

cat > "${CONFIG_FILE}" <<EOF_CONFIG
filesystem_caches:
    query_results:
        path: '${CACHE_DIR}/'
        max_size: '100M'
EOF_CONFIG

settings="use_query_cache = true, query_cache_on_disk_cache_name = 'query_results'"
query="SELECT number FROM numbers(1000) FORMAT Null SETTINGS ${settings}"
written_query="SELECT value > 0 FROM system.events WHERE event = 'QueryCacheOnDiskWrittenBytes'"

echo "-- A gated codec is rejected without its dedicated setting"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}, query_cache_on_disk_codec = 'ZXC'" 2>&1 | grep -o -m1 "enable_zxc_codec"

echo "-- A different codec's dedicated setting does not authorize it"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}, enable_alp_codec = 1, query_cache_on_disk_codec = 'ZXC'" 2>&1 | grep -o -m1 "enable_zxc_codec"

echo "-- The dedicated setting authorizes it"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    ${query}, enable_zxc_codec = 1, query_cache_on_disk_codec = 'ZXC';
    ${written_query};"

echo "-- A codec that requires the column type is rejected even when enabled"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}, enable_alp_codec = 1, query_cache_on_disk_codec = 'ALP'" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}, enable_pco_codec = 1, query_cache_on_disk_codec = 'PCO'" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

rm -rf "${CACHE_DIR}"
rm "${CONFIG_FILE}"
