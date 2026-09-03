#!/usr/bin/env bash
# A corrupt entry of the query cache on disk (setting `query_cache_on_disk_cache_name`) must be a cheap cache miss:
# the query recomputes the correct result, and the corrupt entry is replaced. In particular, absurd serialized counters
# (number of roles, number of chunks) must not lead to huge allocations.
# `clickhouse-local` invocations sharing one cache directory are used so that the entry files can be corrupted between runs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CACHE_DIR="${CLICKHOUSE_TMP}/05024_cache_${CLICKHOUSE_DATABASE}"
CONFIG_FILE="${CLICKHOUSE_TMP}/05024_config_${CLICKHOUSE_DATABASE}.yaml"
rm -rf "${CACHE_DIR}"

cat > "${CONFIG_FILE}" <<EOF
filesystem_caches:
    query_results:
        path: '${CACHE_DIR}/'
        max_size: '100M'
EOF

settings="use_query_cache = true, query_cache_on_disk_cache_name = 'query_results'"
query="SELECT sum(number) FROM numbers(1000) SETTINGS ${settings}"
events_query="SELECT event FROM system.events WHERE event LIKE 'QueryCacheOnDisk%' AND value > 0 ORDER BY event"

echo "-- Compute the result and write it to disk"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}; ${events_query};"

echo "-- Corrupt the body of the entry: it must be handled as a cheap miss"
# The bytes right after the 56-byte fixed header hold the access metadata. Overwriting them with 0xFF breaks the body
# checksum in the fixed header (and would make the serialized role count decode to a huge number).
find "${CACHE_DIR}" -type f -name '0_*' | while read -r file
do
    printf '\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff' \
        | dd of="${file}" bs=1 seek=56 conv=notrunc status=none
done
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}; ${events_query};"

echo "-- The corrupt entry was dropped and replaced, the next run is served from disk"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}; ${events_query};"

echo "-- Truncated payload (the result chunks are cut off) is also a cheap miss"
find "${CACHE_DIR}" -type f -name '0_*' | while read -r file
do
    truncate -s 60 "${file}"
done
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}" 2>/dev/null

rm -rf "${CACHE_DIR}"
rm "${CONFIG_FILE}"
