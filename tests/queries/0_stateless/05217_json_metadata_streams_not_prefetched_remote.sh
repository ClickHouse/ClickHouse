#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
# no-fasttest: needs a remote (S3) disk.
# no-random-settings, no-random-merge-tree-settings: the number of prefetches depends on the read and
# serialization settings.

# Metadata streams of `JSON` and `Dynamic` columns (`object_structure`, `dynamic_structure`) are read only
# while deserializing the prefix, which always reads from the beginning of the file, and are released right
# after that. Prefetching such a stream for a granule that is not the first one is a read at the wrong
# offset that only reads the file a second time: on object storage every file is a network round trip.
# `05111_json_metadata_streams_not_reopened` checks this on a local disk, where the second read shows up as
# an extra opened file. Here the part is on a remote disk, where the prefetch is governed by
# `remote_filesystem_read_prefetch` and the number of issued prefetches is the direct measure.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The rows are inline instead of `SELECT ... FROM numbers(5)`: the AST fuzzer of the stress test can grow the
# row count to ~1M, and with `index_granularity = 1` every row is a granule that writes all streams of the `JSON`
# column. Under sanitizers that takes tens of minutes, and the writing of a block cannot be cancelled midway.
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_json_metadata_streams_remote;
    CREATE TABLE t_json_metadata_streams_remote (t UInt32, json JSON)
    ENGINE = MergeTree ORDER BY t
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        ratio_of_defaults_for_sparse_serialization = 1, index_granularity = 1,
        prewarm_mark_cache = 0, storage_policy = 's3_no_cache';

    SYSTEM STOP MERGES t_json_metadata_streams_remote;

    INSERT INTO t_json_metadata_streams_remote VALUES (0, '{\"a\":0,\"b\":\"s0\",\"c\":[0],\"d\":0,\"e\":true}'), (1, '{\"a\":1,\"b\":\"s1\",\"c\":[1],\"d\":0.5,\"e\":true}'), (2, '{\"a\":2,\"b\":\"s2\",\"c\":[2],\"d\":1,\"e\":true}'), (3, '{\"a\":3,\"b\":\"s3\",\"c\":[3],\"d\":1.5,\"e\":true}'), (4, '{\"a\":4,\"b\":\"s4\",\"c\":[4],\"d\":2,\"e\":true}');
"

function prefetches_for_granule()
{
    local granule="$1"
    local query_id
    query_id="05217_${granule}_$(${CLICKHOUSE_CLIENT} -q "SELECT lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")"

    ${CLICKHOUSE_CLIENT} --query_id "${query_id}" -q "
        SELECT json FROM t_json_metadata_streams_remote WHERE t = ${granule} FORMAT Null
        SETTINGS max_threads = 1, allow_prefetched_read_pool_for_remote_filesystem = 1,
            remote_filesystem_read_prefetch = 1, remote_filesystem_read_method = 'threadpool',
            filesystem_prefetch_step_marks = 1, filesystem_prefetches_limit = 0,
            load_marks_asynchronously = 0, use_uncompressed_cache = 0, enable_parallel_replicas = 0,
            merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1
    "

    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

    ${CLICKHOUSE_CLIENT} -q "
        SELECT 'prefetches (granule ${granule})', ProfileEvents['RemoteFSPrefetches']
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '${query_id}' AND type = 'QueryFinish'
    "
}

# The first granule keeps the prefetch of the metadata streams: the prefix is not deserialized yet and
# reuses exactly that prefetch. A later granule must not have it.
prefetches_for_granule 0
prefetches_for_granule 4

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_json_metadata_streams_remote"
