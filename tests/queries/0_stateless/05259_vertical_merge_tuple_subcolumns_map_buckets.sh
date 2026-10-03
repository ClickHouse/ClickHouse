#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-object-storage, no-shared-merge-tree, no-replicated-database, no-parallel-replicas
#
# no-random-merge-tree-settings: pins flatten, Vertical activation, and Map bucket settings.
# no-object-storage / no-shared-merge-tree: reads columns_substreams.txt from a local part directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_tuple_map_buckets" 2>/dev/null || true
}
trap cleanup EXIT

cleanup

# Two gathering leaves, so Vertical runs only after the Tuple is flattened.
# An empty Map enumerates one bucket; these rows average 64 keys, so sqrt bucketing
# opens several buckets and writes bucket_indexes. columns_substreams.txt must list
# that stream or later reads of the merged part disagree with the files on disk.
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_tuple_map_buckets
    (
        k UInt64,
        t Tuple(m Map(String, UInt64), s String)
    )
    ENGINE = MergeTree
    ORDER BY k
    SETTINGS
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0,
        enable_block_number_column = 0,
        enable_block_offset_column = 0,
        replace_long_file_name_to_hash = 0,
        vertical_merge_algorithm_min_rows_to_activate = 1,
        vertical_merge_algorithm_min_columns_to_activate = 2,
        allow_experimental_vertical_merge_tuple_subcolumns = 1,
        map_serialization_version = 'with_buckets',
        map_serialization_version_for_zero_level_parts = 'with_buckets',
        map_buckets_strategy = 'sqrt',
        map_buckets_coefficient = 1,
        map_buckets_min_avg_size = 1,
        max_buckets_in_map = 8,
        auto_statistics_types = '';

    SYSTEM STOP MERGES t_tuple_map_buckets;
    INSERT INTO t_tuple_map_buckets
        SELECT 1, (mapFromArrays(arrayMap(i -> toString(i), range(64)), arrayMap(i -> toUInt64(i), range(64))), 'keep');
    INSERT INTO t_tuple_map_buckets
        SELECT 2, (mapFromArrays(arrayMap(i -> toString(i), range(64)), arrayMap(i -> toUInt64(i), range(64))), 'keep');
    SYSTEM START MERGES t_tuple_map_buckets;
    OPTIMIZE TABLE t_tuple_map_buckets FINAL;
"

echo 'rows'
${CLICKHOUSE_CLIENT} -q "
    SELECT k, t.s, length(t.m), arraySum(mapValues(t.m))
    FROM t_tuple_map_buckets
    ORDER BY k
"

echo 'check'
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_tuple_map_buckets SETTINGS check_query_single_value_result = 1"

echo 'merge_algorithm'
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS part_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT merge_algorithm
    FROM system.part_log
    WHERE database = currentDatabase()
      AND table = 't_tuple_map_buckets'
      AND event_type = 'MergeParts'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
"

echo 'bucket_indexes'
PART_PATH=$(${CLICKHOUSE_CLIENT} -q "
    SELECT path
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_tuple_map_buckets' AND active
")
if grep -q 'bucket_indexes' "${PART_PATH}/columns_substreams.txt"; then
    echo present
else
    echo missing
    exit 1
fi
