#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
# The exact number of streams and marks files depends on the serialization settings.

# Streams whose data file is empty (for example, `SharedVariant` of a `JSON` path whose values all have one type)
# have nothing to read, so their marks files must not be loaded. On object storage every marks file is a network
# round trip, and such streams are about 40% of the files of a part with a `JSON` column.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Two identical tables: one for the synchronous marks loading path, one for the asynchronous one.
# Separate tables let both queries start with none of their marks in the cache without clearing the
# (server-wide) mark cache in between.
# The rows are inline instead of `SELECT ... FROM numbers(5)`: the AST fuzzer of the stress test can grow the
# row count to ~1M, and with `index_granularity = 1` every row is a granule that writes all streams of the `JSON`
# column. Under sanitizers that takes tens of minutes, and the writing of a block cannot be cancelled midway.
for suffix in sync async
do
    ${CLICKHOUSE_CLIENT} -q "
        DROP TABLE IF EXISTS t_json_empty_streams_${suffix};
        CREATE TABLE t_json_empty_streams_${suffix} (t UInt32, json JSON)
        ENGINE = MergeTree ORDER BY t
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
            ratio_of_defaults_for_sparse_serialization = 1, index_granularity = 1, prewarm_mark_cache = 0;

        SYSTEM STOP MERGES t_json_empty_streams_${suffix};

        INSERT INTO t_json_empty_streams_${suffix} VALUES (0, '{\"a\":0,\"b\":\"s0\",\"c\":[0],\"d\":0,\"e\":true}'), (1, '{\"a\":1,\"b\":\"s1\",\"c\":[1],\"d\":0.5,\"e\":true}'), (2, '{\"a\":2,\"b\":\"s2\",\"c\":[2],\"d\":1,\"e\":true}'), (3, '{\"a\":3,\"b\":\"s3\",\"c\":[3],\"d\":1.5,\"e\":true}'), (4, '{\"a\":4,\"b\":\"s4\",\"c\":[4],\"d\":2,\"e\":true}');
    "
done

# Every path has values of a single type, so the `SharedVariant` streams of all paths are empty.
${CLICKHOUSE_CLIENT} -q "
    SELECT 'streams', sum(length(substreams)) FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_json_empty_streams_sync' AND active
"

run_and_report()
{
    local suffix="$1"
    local async="$2"

    local query_id
    query_id="05109_${suffix}_$(${CLICKHOUSE_CLIENT} -q "SELECT lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")"

    ${CLICKHOUSE_CLIENT} --query_id "${query_id}" -q "
        SELECT json FROM t_json_empty_streams_${suffix} WHERE t = 4 FORMAT Null
        SETTINGS max_threads = 1, load_marks_asynchronously = ${async}, local_filesystem_read_prefetch = 1,
            use_uncompressed_cache = 0, enable_parallel_replicas = 0
    "

    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

    # `LoadedMarksFiles` counts the marks files that were actually read, both on the synchronous path and
    # in the background marks loading tasks (they run under the thread group of the query).
    ${CLICKHOUSE_CLIENT} -q "
        SELECT 'marks files loaded (${suffix})', ProfileEvents['LoadedMarksFiles']
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '${query_id}' AND type = 'QueryFinish'
    "
}

run_and_report sync 0
run_and_report async 1

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE t_json_empty_streams_sync;
    DROP TABLE t_json_empty_streams_async;
"
