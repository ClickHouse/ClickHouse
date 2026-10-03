#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-replicated-database, no-parallel-replicas
#
# no-random-merge-tree-settings: pins flatten settings and Vertical activation.
#
# A minmax skip index on Tuple leaf `t.s` with ORDER BY k keeps `t` in gathering
# columns. Flatten re-keys the index onto `t.s` so Vertical gather rebuilds it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

COMMON_SETTINGS="
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    replace_long_file_name_to_hash = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    allow_experimental_vertical_merge_tuple_subcolumns = 1,
    auto_statistics_types = ''
"

print_merge_algorithm()
{
    local table="$1"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS part_log"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT merge_algorithm
        FROM system.part_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND database = currentDatabase() AND table = '${table}' AND event_type = 'MergeParts'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    "
}

print_skip_index_materialized()
{
    local table="$1"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT secondary_indices_compressed_bytes > 0
        FROM system.parts
        WHERE database = currentDatabase() AND table = '${table}' AND active
    "
}

echo '=== minmax on t.s with ORDER BY k flattens and rebuilds ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_idx_leaf;
    CREATE TABLE t_idx_leaf
    (
        k UInt64,
        t Tuple(s String, n UInt8),
        INDEX idx t.s TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        index_granularity = 1,
        packed_skip_index_max_bytes = 0;

    INSERT INTO t_idx_leaf VALUES (1, ('a', 1)), (2, ('b', 2));
    INSERT INTO t_idx_leaf VALUES (3, ('c', 3)), (4, ('d', 4));
    OPTIMIZE TABLE t_idx_leaf FINAL;
    SELECT count() FROM t_idx_leaf WHERE t.s = 'c' SETTINGS force_data_skipping_indices = 'idx';
    SELECT k, t FROM t_idx_leaf ORDER BY k;
    CHECK TABLE t_idx_leaf SETTINGS check_query_single_value_result = 1;
"
echo 'merge_algorithm'
print_merge_algorithm t_idx_leaf
echo 'skip_index_materialized'
print_skip_index_materialized t_idx_leaf
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_idx_leaf;"
