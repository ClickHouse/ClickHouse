#!/usr/bin/env bash
# `SYSTEM DROP QUERY CACHE` and `SYSTEM DROP QUERY CACHE TAG` also remove the entries of the query cache on disk from the filesystem
# cache selected by setting `query_cache_on_disk_cache_name`. Separate `clickhouse-local` processes sharing one cache directory make sure
# that the entries are read from disk (the in-memory query cache is disabled in `clickhouse-local` anyway).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CACHE_DIR="${CLICKHOUSE_TMP}/05316_cache_${CLICKHOUSE_DATABASE}"
CONFIG_FILE="${CLICKHOUSE_TMP}/05316_config_${CLICKHOUSE_DATABASE}.yaml"
rm -rf "${CACHE_DIR}"

cat > "${CONFIG_FILE}" <<EOF_CONFIG
filesystem_caches:
    query_results:
        path: '${CACHE_DIR}/'
        max_size: '100M'
EOF_CONFIG

settings="use_query_cache = true, query_cache_on_disk_cache_name = 'query_results'"

function write_entries()
{
    ${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
        SELECT 'untagged' SETTINGS ${settings} FORMAT Null;
        SELECT 'tagged' SETTINGS ${settings}, query_cache_tag = 'a' FORMAT Null;
        SELECT 'tagged' SETTINGS ${settings}, query_cache_tag = 'b' FORMAT Null;"
}

# Every query runs in its own process, which reports whether it was served from the query cache on disk.
function check_entry()
{
    ${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
        $2 SETTINGS ${settings}$3, enable_writes_to_query_cache_on_disk = false FORMAT Null;
        SELECT '$1', if(sum(value) > 0, 'hit', 'miss') FROM system.events WHERE event = 'QueryCacheOnDiskHits';"
}

function check_entries()
{
    check_entry "untagged" "SELECT 'untagged'" ""
    check_entry "tag a" "SELECT 'tagged'" ", query_cache_tag = 'a'"
    check_entry "tag b" "SELECT 'tagged'" ", query_cache_tag = 'b'"
}

echo "-- The entries are served from disk"
write_entries
check_entries

echo "-- DROP QUERY CACHE TAG removes only the entries with the tag"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "SET query_cache_on_disk_cache_name = 'query_results'; SYSTEM DROP QUERY CACHE TAG 'a';"
check_entries

echo "-- Without setting query_cache_on_disk_cache_name, DROP QUERY CACHE does not touch the query cache on disk"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "SYSTEM DROP QUERY CACHE;"
check_entries

echo "-- DROP QUERY CACHE removes all entries"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "SET query_cache_on_disk_cache_name = 'query_results'; SYSTEM DROP QUERY CACHE;"
check_entries

echo "-- New entries can be written afterwards"
write_entries
check_entries

rm -rf "${CACHE_DIR}"
rm "${CONFIG_FILE}"
