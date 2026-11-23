#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

# Metadata streams of `JSON` and `Dynamic` columns (`object_structure`, `dynamic_structure`) are read only while
# deserializing the prefix, which always reads from the beginning of the file, and are released right after that.
# The prefetch for the current mark must not create them again and read the files a second time: on object storage
# every file is a network round trip.
# So a read of a granule that is not the first one must not open more files with prefetch than without it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_json_metadata_streams;
    CREATE TABLE t_json_metadata_streams (t UInt32, json JSON)
    ENGINE = MergeTree ORDER BY t
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1, index_granularity = 1;

    SYSTEM STOP MERGES t_json_metadata_streams;

    INSERT INTO t_json_metadata_streams SELECT number, concat('{\"a\":', toString(number), ',\"b\":\"s', toString(number), '\",\"c\":[', toString(number), '],\"d\":', toString(number / 2), ',\"e\":true}')::JSON FROM numbers(5);
"

# Read a granule that is not the first one and count the opened files. The mark cache is cleared before every query,
# so that the number of opened mark files is the same in both runs.
function read_with_prefetch()
{
    local prefetch="$1"
    local query_id
    query_id="05111_${prefetch}_$(${CLICKHOUSE_CLIENT} -q "SELECT lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")"

    ${CLICKHOUSE_CLIENT} -q "SYSTEM CLEAR MARK CACHE"

    ${CLICKHOUSE_CLIENT} --query_id "${query_id}" -q "
        SELECT json FROM t_json_metadata_streams WHERE t = 4 FORMAT Null
        SETTINGS max_threads = 1, load_marks_asynchronously = 0, local_filesystem_read_prefetch = ${prefetch}, use_uncompressed_cache = 0, enable_parallel_replicas = 0
    "

    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

    ${CLICKHOUSE_CLIENT} -q "
        SELECT ProfileEvents['FileOpen']
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '${query_id}' AND type = 'QueryFinish'
    "
}

# Warm up: the very first read of a part also opens its metadata files, and that must not be counted.
read_with_prefetch 0 > /dev/null

WITHOUT_PREFETCH=$(read_with_prefetch 0)
WITH_PREFETCH=$(read_with_prefetch 1)

${CLICKHOUSE_CLIENT} -q "SELECT 'prefetch does not open more files', ${WITH_PREFETCH} <= ${WITHOUT_PREFETCH}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_json_metadata_streams"
