#!/usr/bin/env bash
# A corrupt entry of the query cache on disk (setting `query_cache_on_disk_cache_name`) must also be replaced when the
# read path never looks at it, i.e. with `enable_reads_from_query_cache_on_disk = 0`. Otherwise the broken entry would
# keep the write path skipping the insert ("a non-stale query result already exists") until the entry expires.
# `clickhouse-local` invocations sharing one cache directory are used so that the entry files can be corrupted between runs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CACHE_DIR="${CLICKHOUSE_TMP}/05044_cache_${CLICKHOUSE_DATABASE}"
CONFIG_FILE="${CLICKHOUSE_TMP}/05044_config_${CLICKHOUSE_DATABASE}.yaml"
rm -rf "${CACHE_DIR}"

cat > "${CONFIG_FILE}" <<EOF
filesystem_caches:
    query_results:
        path: '${CACHE_DIR}/'
        max_size: '100M'
EOF

settings="use_query_cache = true, query_cache_on_disk_cache_name = 'query_results'"
query="SELECT sum(number) FROM numbers(1000) SETTINGS ${settings}"
query_write_only="SELECT sum(number) FROM numbers(1000) SETTINGS ${settings}, enable_reads_from_query_cache_on_disk = 0"
events_query="SELECT event FROM system.events WHERE event LIKE 'QueryCacheOnDisk%' AND value > 0 ORDER BY event"

echo "-- Compute the result and write it to disk"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}; ${events_query};"

echo "-- Corrupt the body of the entry, leaving the fixed header intact"
# The fixed header is 56 bytes; everything after it is covered by the checksum in the header.
find "${CACHE_DIR}" -type f -name '0_*' | while read -r file
do
    printf '\xff\xff\xff\xff\xff\xff\xff\xff' | dd of="${file}" bs=1 seek=56 conv=notrunc status=none
done

echo "-- With reads from disk disabled, the write must still replace the corrupt entry"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query_write_only}; ${events_query};"

echo "-- The replacement is a valid entry: the next run is served from disk"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${query}; ${events_query};"

rm -rf "${CACHE_DIR}"
rm "${CONFIG_FILE}"
