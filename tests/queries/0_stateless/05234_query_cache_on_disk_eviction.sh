#!/usr/bin/env bash
# The entries of the query cache on disk (setting `query_cache_on_disk_cache_name`) are ordinary entries of the filesystem cache:
# they are split into file segments, evicted when the filesystem cache runs out of space, not stored at all when they do not fit, and
# removed by `SYSTEM DROP FILESYSTEM CACHE '<name>'`. Tested with `clickhouse-local` and tiny filesystem caches with small file segments.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CACHE_DIR="${CLICKHOUSE_TMP}/05234_cache_${CLICKHOUSE_DATABASE}"
CONFIG_FILE="${CLICKHOUSE_TMP}/05234_config_${CLICKHOUSE_DATABASE}.yaml"
rm -rf "${CACHE_DIR}"

# Each cache holds two entries of the queries below (about 32 KiB each, uncompressed), not three. `LRU` has a single queue, so
# what gets evicted is predictable; the default `SLRU` admits new entries only into its probationary queue, which is smaller.
cat > "${CONFIG_FILE}" <<EOF_CONFIG
filesystem_caches:
    lru:
        path: '${CACHE_DIR}/lru/'
        max_size: '64Ki'
        max_file_segment_size: '4Ki'
        boundary_alignment: '4Ki'
        cache_policy: 'LRU'
    slru:
        path: '${CACHE_DIR}/slru/'
        max_size: '64Ki'
        max_file_segment_size: '4Ki'
        boundary_alignment: '4Ki'
EOF_CONFIG

# No compression, so that the sizes of the entries are predictable. The tag makes the queries distinct entries.
# The result is 1000 rows of 8 + 17 bytes.
query="SELECT number, hex(sipHash64(number)) FROM numbers(1000) FORMAT Null SETTINGS use_query_cache = true, query_cache_on_disk_codec = 'NONE', query_cache_on_disk_cache_name = "
events_query="SELECT event, value FROM system.events WHERE event LIKE 'QueryCacheOnDisk%' AND event NOT LIKE '%Bytes' ORDER BY event"

echo "-- An entry is stored in several file segments and served from them"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    ${query} 'lru', query_cache_tag = 'a';
    SELECT count() > 1, sum(size) > 16384 FROM system.filesystem_cache WHERE cache_name = 'lru';
    ${query} 'lru', query_cache_tag = 'a';
    ${events_query};"

echo "-- Two more entries push the first one out of the cache: it is a miss and is written again"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    ${query} 'lru', query_cache_tag = 'b';
    ${query} 'lru', query_cache_tag = 'c';
    ${query} 'lru', query_cache_tag = 'a';
    ${query} 'lru', query_cache_tag = 'a';
    ${events_query};"

echo "-- An entry bigger than the whole cache is not stored, the query still works"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    SELECT count() FROM (SELECT number, hex(sipHash64(number)) FROM numbers(10000) SETTINGS use_query_cache = true, query_cache_on_disk_codec = 'NONE', query_cache_on_disk_cache_name = 'lru');
    SELECT count() FROM (SELECT number, hex(sipHash64(number)) FROM numbers(10000) SETTINGS use_query_cache = true, query_cache_on_disk_codec = 'NONE', query_cache_on_disk_cache_name = 'lru');
    SELECT event, value FROM system.events WHERE event LIKE 'QueryCacheOnDisk%' ORDER BY event;"

echo "-- An entry which does not fit into the probationary queue of an SLRU cache is not stored and leaves no file segments behind"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    ${query} 'slru', query_cache_tag = 'a';
    SELECT count() FROM system.filesystem_cache WHERE cache_name = 'slru';
    ${query} 'slru', query_cache_tag = 'a';
    SELECT event, value FROM system.events WHERE event LIKE 'QueryCacheOnDisk%' ORDER BY event;"

echo "-- SYSTEM DROP FILESYSTEM CACHE '<name>' removes the entries"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    ${query} 'lru', query_cache_tag = 'd';
    ${query} 'lru', query_cache_tag = 'd';
    SYSTEM DROP FILESYSTEM CACHE 'lru';
    SELECT count() FROM system.filesystem_cache WHERE cache_name = 'lru';
    ${query} 'lru', query_cache_tag = 'd';
    ${events_query};"

rm -rf "${CACHE_DIR}"
rm "${CONFIG_FILE}"
