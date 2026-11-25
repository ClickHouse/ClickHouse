#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3 (minio)

# `SYSTEM DROP QUERY CACHE` and `SYSTEM DROP QUERY CACHE TAG` remove only the entries of the query cache on disk from the filesystem
# cache selected by setting `query_cache_on_disk_cache_name`, and leave the other data of the same filesystem cache in place. The other
# data here is a file read with the `s3` table function through the same filesystem cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CACHE_DIR="${CLICKHOUSE_TMP}/05321_cache_${CLICKHOUSE_DATABASE}"
CONFIG_FILE="${CLICKHOUSE_TMP}/05321_config_${CLICKHOUSE_DATABASE}.yaml"
S3_URL="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}.tsv"
rm -rf "${CACHE_DIR}"

cat > "${CONFIG_FILE}" <<EOF_CONFIG
filesystem_caches:
    query_results:
        path: '${CACHE_DIR}/'
        max_size: '100M'
EOF_CONFIG

settings="use_query_cache = true, query_cache_on_disk_cache_name = 'query_results'"

echo "-- Put other data into the filesystem cache"
${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION s3('${S3_URL}', 'test', 'testtest', 'TSV', 'n UInt64') SELECT number FROM numbers(1000) SETTINGS s3_truncate_on_insert = 1"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    SELECT sum(n) FROM s3('${S3_URL}', 'test', 'testtest', 'TSV', 'n UInt64')
    SETTINGS filesystem_cache_name = 'query_results', enable_filesystem_cache = 1"

keys_query="SELECT arrayStringConcat(arraySort(groupUniqArray(key)), ',') FROM system.filesystem_cache WHERE cache_name = 'query_results'"
other_keys=$(${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "${keys_query}")
if [ -n "${other_keys}" ]; then echo "the other data is cached"; else echo "the other data is not cached"; fi

# Every check runs in its own process: whether all keys of the other data are still there, and how many keys belong to the query cache.
function check_keys()
{
    ${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
        WITH splitByChar(',', '${other_keys}') AS other
        SELECT hasAll(groupUniqArray(key), other) AS other_data_intact, uniqExact(key) - length(other) AS query_cache_keys
        FROM system.filesystem_cache WHERE cache_name = 'query_results'
        FORMAT TSVWithNames"
}

echo "-- Write entries of the query cache on disk"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "
    SELECT 'untagged' SETTINGS ${settings} FORMAT Null;
    SELECT 'tagged' SETTINGS ${settings}, query_cache_tag = 'a' FORMAT Null;"
check_keys

echo "-- DROP QUERY CACHE TAG"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "SET query_cache_on_disk_cache_name = 'query_results'; SYSTEM DROP QUERY CACHE TAG 'a';"
check_keys

echo "-- DROP QUERY CACHE"
${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --query "SET query_cache_on_disk_cache_name = 'query_results'; SYSTEM DROP QUERY CACHE;"
check_keys

rm -rf "${CACHE_DIR}"
rm "${CONFIG_FILE}"
