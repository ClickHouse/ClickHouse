#!/usr/bin/env bash
# The integrity check of an entry of the query cache on disk (setting `query_cache_on_disk_cache_name`) covers the fixed
# header as well, not only the body: `parseFixedHeader` has to trust `total_size` and `expires_at` before the checksum can
# be verified, so a corruption of a header field alone must not be able to change the freshness semantics of the entry.
# Every corruption below leaves the body untouched and flips only one header field, and must still be a cache miss.
# `clickhouse-local` invocations sharing one cache directory are used so that the entry files can be corrupted between runs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CACHE_DIR="${CLICKHOUSE_TMP}/05153_cache_${CLICKHOUSE_DATABASE}"
CONFIG_FILE="${CLICKHOUSE_TMP}/05153_config_${CLICKHOUSE_DATABASE}.yaml"
rm -rf "${CACHE_DIR}"

cat > "${CONFIG_FILE}" <<EOF
filesystem_caches:
    query_results:
        path: '${CACHE_DIR}/'
        max_size: '100M'
EOF

settings="use_query_cache = true, query_cache_on_disk_cache_name = 'query_results'"
query="SELECT sum(number) FROM numbers(1000) SETTINGS ${settings}"
events_query="SELECT event FROM system.events WHERE event LIKE 'QueryCacheOnDisk%Misses' OR event LIKE 'QueryCacheOnDisk%Hits' ORDER BY event"

# The fixed header is 56 bytes: magic[8], format_version (UInt32), protocol_revision (UInt32), total_size (UInt64) at
# offset 16, created_at (UInt64) at offset 24, expires_at (UInt64) at offset 32, checksum (UInt128) at offset 40.
corrupt_header_field() # $1 = offset of the UInt64 field, $2 = the 8 replacement bytes
{
    find "${CACHE_DIR}" -type f -name '0_*' | while read -r file
    do
        printf "$2" | dd of="${file}" bs=1 seek="$1" conv=notrunc status=none
    done
}

echo "-- Compute the result and write it to disk"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}; ${events_query};"

echo "-- The entry is served from disk while its header is intact"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}; ${events_query};"

echo "-- Moving expires_at into the far future is a miss, not a resurrected entry"
corrupt_header_field 32 '\xff\xff\xff\xff\x00\x00\x00\x00' # year 2106
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}; ${events_query};"

echo "-- The corrupt entry was replaced, so the next run is a hit again"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}; ${events_query};"

echo "-- Changing created_at is a miss as well"
corrupt_header_field 24 '\x01\x00\x00\x00\x00\x00\x00\x00'
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}; ${events_query};"

echo "-- And so is changing total_size"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}" > /dev/null
corrupt_header_field 16 '\x39\x00\x00\x00\x00\x00\x00\x00' # 57, one byte past the fixed header
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}; ${events_query};"

rm -rf "${CACHE_DIR}"
rm "${CONFIG_FILE}"
