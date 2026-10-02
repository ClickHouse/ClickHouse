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
for suffix in sync async
do
    ${CLICKHOUSE_CLIENT} -q "
        DROP TABLE IF EXISTS t_json_empty_streams_${suffix};
        CREATE TABLE t_json_empty_streams_${suffix} (t UInt32, json JSON)
        ENGINE = MergeTree ORDER BY t
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
            ratio_of_defaults_for_sparse_serialization = 1, index_granularity = 1, prewarm_mark_cache = 0;

        SYSTEM STOP MERGES t_json_empty_streams_${suffix};

        INSERT INTO t_json_empty_streams_${suffix} SELECT number, concat('{\"a\":', toString(number), ',\"b\":\"s', toString(number), '\",\"c\":[', toString(number), '],\"d\":', toString(number / 2), ',\"e\":true}')::JSON FROM numbers(5);
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
