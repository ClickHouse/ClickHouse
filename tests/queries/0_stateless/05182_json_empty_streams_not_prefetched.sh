#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
# no-fasttest: needs a remote (S3) disk - only reads from a remote filesystem go through the prefetched read pool.
# The exact number of streams and prefetches depends on the serialization settings.

# Streams whose data file is empty (for example, `SharedVariant` of a `JSON` path whose values all have one type)
# have nothing to read, so neither their marks nor a prefetch are needed. `05109_json_empty_streams_marks_not_loaded`
# checks this on a local disk; this test checks it on a remote disk, where the reading goes through
# `MergeTreePrefetchedReadPool` (a different code path) and where every marks file is a network round trip.
#
# `RemoteFSPrefetches` is pinned as well: it does not go down (`AsynchronousBoundedReadBuffer::prefetch` already
# does nothing when there is no data to read), but it must not go up either.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_json_empty_streams_prefetch;
    CREATE TABLE t_json_empty_streams_prefetch (t UInt32, json JSON)
    ENGINE = MergeTree ORDER BY t
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        ratio_of_defaults_for_sparse_serialization = 1, index_granularity = 1,
        prewarm_mark_cache = 0, storage_policy = 's3_no_cache';

    SYSTEM STOP MERGES t_json_empty_streams_prefetch;

    INSERT INTO t_json_empty_streams_prefetch SELECT number, concat('{\"a\":', toString(number), ',\"b\":\"s', toString(number), '\",\"c\":[', toString(number), '],\"d\":', toString(number / 2), ',\"e\":true}')::JSON FROM numbers(5);
"

# Every path has values of a single type, so the `SharedVariant` streams of all paths are empty.
${CLICKHOUSE_CLIENT} -q "
    SELECT 'streams', sum(length(substreams)) FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_json_empty_streams_prefetch' AND active
"

query_id="05182_$(${CLICKHOUSE_CLIENT} -q "SELECT lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")"

${CLICKHOUSE_CLIENT} --query_id "${query_id}" -q "
    SELECT json FROM t_json_empty_streams_prefetch FORMAT Null
    SETTINGS max_threads = 1, allow_prefetched_read_pool_for_remote_filesystem = 1,
        remote_filesystem_read_prefetch = 1, remote_filesystem_read_method = 'threadpool',
        filesystem_prefetch_step_marks = 1, filesystem_prefetches_limit = 0,
        load_marks_asynchronously = 0, use_uncompressed_cache = 0, enable_parallel_replicas = 0,
        merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1
"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} -q "
    SELECT 'marks files loaded', ProfileEvents['LoadedMarksFiles'], 'prefetches', ProfileEvents['RemoteFSPrefetches']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${query_id}' AND type = 'QueryFinish'
"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_json_empty_streams_prefetch"
