#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-replicated-database, no-parallel-replicas
#
# Tuple flattening follows the index/statistics work that this merge will
# actually materialize, not every metadata declaration on the table.

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
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 2,
    allow_experimental_vertical_merge_tuple_subcolumns = 1,
    auto_statistics_types = ''
"

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "
        DROP TABLE IF EXISTS t_excluded_parent_index;
        DROP TABLE IF EXISTS t_no_materialize_parent_index;
        DROP TABLE IF EXISTS t_active_parent_index;
        DROP TABLE IF EXISTS t_no_materialize_multi_index;
        DROP TABLE IF EXISTS t_active_multi_index;
        DROP TABLE IF EXISTS t_leaf_text_index;
        DROP TABLE IF EXISTS t_parent_text_index;
        DROP TABLE IF EXISTS t_no_materialize_parent_stats;
        DROP TABLE IF EXISTS t_virtual_indices;
    " 2>/dev/null || true
}
trap cleanup EXIT

print_merge_algorithm()
{
    local table="$1"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS part_log"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT merge_algorithm
        FROM system.part_log
        WHERE database = currentDatabase()
          AND table = '${table}'
          AND event_type = 'MergeParts'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    "
}

cleanup

echo '=== excluded parent index does not pin ==='
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_excluded_parent_index
    (
        k UInt64,
        t Tuple(x String, y String),
        INDEX idx cityHash64(t) TYPE bloom_filter(0.01) GRANULARITY 1
    )
    ENGINE = MergeTree
    ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        exclude_materialize_skip_indexes_on_merge = 'idx';

    SYSTEM STOP MERGES t_excluded_parent_index;
    INSERT INTO t_excluded_parent_index VALUES (1, ('a', 'b'));
    INSERT INTO t_excluded_parent_index VALUES (2, ('c', 'd'));
    SYSTEM START MERGES t_excluded_parent_index;
    OPTIMIZE TABLE t_excluded_parent_index FINAL;
    SELECT count() FROM t_excluded_parent_index;
    CHECK TABLE t_excluded_parent_index SETTINGS check_query_single_value_result = 1;
"
print_merge_algorithm t_excluded_parent_index

echo
echo '=== disabled skip-index materialization does not pin ==='
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_no_materialize_parent_index
    (
        k UInt64,
        t Tuple(x String, y String),
        INDEX idx cityHash64(t) TYPE bloom_filter(0.01) GRANULARITY 1
    )
    ENGINE = MergeTree
    ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        materialize_skip_indexes_on_merge = 0;

    SYSTEM STOP MERGES t_no_materialize_parent_index;
    INSERT INTO t_no_materialize_parent_index VALUES (1, ('a', 'b'));
    INSERT INTO t_no_materialize_parent_index VALUES (2, ('c', 'd'));
    SYSTEM START MERGES t_no_materialize_parent_index;
    OPTIMIZE TABLE t_no_materialize_parent_index FINAL;
    SELECT count() FROM t_no_materialize_parent_index;
    CHECK TABLE t_no_materialize_parent_index SETTINGS check_query_single_value_result = 1;
"
print_merge_algorithm t_no_materialize_parent_index

echo
echo '=== active parent index pins ==='
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_active_parent_index
    (
        k UInt64,
        t Tuple(x String, y String),
        INDEX idx cityHash64(t) TYPE bloom_filter(0.01) GRANULARITY 1
    )
    ENGINE = MergeTree
    ORDER BY k
    SETTINGS ${COMMON_SETTINGS};

    SYSTEM STOP MERGES t_active_parent_index;
    INSERT INTO t_active_parent_index VALUES (1, ('a', 'b'));
    INSERT INTO t_active_parent_index VALUES (2, ('c', 'd'));
    SYSTEM START MERGES t_active_parent_index;
    OPTIMIZE TABLE t_active_parent_index FINAL;
    SELECT count() FROM t_active_parent_index;
    CHECK TABLE t_active_parent_index SETTINGS check_query_single_value_result = 1;
"
print_merge_algorithm t_active_parent_index

echo
echo '=== disabled multi-column index does not pin ==='
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_no_materialize_multi_index
    (
        k UInt64,
        t Tuple(x String, y String),
        v String,
        INDEX idx cityHash64(t, v) TYPE bloom_filter(0.01) GRANULARITY 1
    )
    ENGINE = MergeTree
    ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        vertical_merge_algorithm_min_columns_to_activate = 3,
        materialize_skip_indexes_on_merge = 0;

    SYSTEM STOP MERGES t_no_materialize_multi_index;
    INSERT INTO t_no_materialize_multi_index VALUES (1, ('a', 'b'), 'x');
    INSERT INTO t_no_materialize_multi_index VALUES (2, ('c', 'd'), 'y');
    SYSTEM START MERGES t_no_materialize_multi_index;
    OPTIMIZE TABLE t_no_materialize_multi_index FINAL;
    SELECT count() FROM t_no_materialize_multi_index;
    CHECK TABLE t_no_materialize_multi_index SETTINGS check_query_single_value_result = 1;
"
print_merge_algorithm t_no_materialize_multi_index

echo
echo '=== active multi-column index pins ==='
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_active_multi_index
    (
        k UInt64,
        t Tuple(x String, y String),
        v String,
        INDEX idx cityHash64(t, v) TYPE bloom_filter(0.01) GRANULARITY 1
    )
    ENGINE = MergeTree
    ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        vertical_merge_algorithm_min_columns_to_activate = 3;

    SYSTEM STOP MERGES t_active_multi_index;
    INSERT INTO t_active_multi_index VALUES (1, ('a', 'b'), 'x');
    INSERT INTO t_active_multi_index VALUES (2, ('c', 'd'), 'y');
    SYSTEM START MERGES t_active_multi_index;
    OPTIMIZE TABLE t_active_multi_index FINAL;
    SELECT count() FROM t_active_multi_index;
    CHECK TABLE t_active_multi_index SETTINGS check_query_single_value_result = 1;
"
print_merge_algorithm t_active_multi_index

echo
echo '=== leaf text index does not pin ==='
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_leaf_text_index
    (
        k UInt64,
        t Tuple(x String, y String),
        INDEX idx t.x TYPE text(tokenizer = ngrams(3)) GRANULARITY 1
    )
    ENGINE = MergeTree
    ORDER BY k
    SETTINGS ${COMMON_SETTINGS};

    SYSTEM STOP MERGES t_leaf_text_index;
    INSERT INTO t_leaf_text_index VALUES (1, ('abc', 'def'));
    INSERT INTO t_leaf_text_index VALUES (2, ('ghi', 'jkl'));
    SYSTEM START MERGES t_leaf_text_index;
    OPTIMIZE TABLE t_leaf_text_index FINAL;
    SELECT count() FROM t_leaf_text_index;
    CHECK TABLE t_leaf_text_index SETTINGS check_query_single_value_result = 1;
"
print_merge_algorithm t_leaf_text_index

echo
echo '=== parent text expression pins ==='
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_parent_text_index
    (
        k UInt64,
        t Tuple(x String, y String),
        INDEX idx toString(t) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1
    )
    ENGINE = MergeTree
    ORDER BY k
    SETTINGS ${COMMON_SETTINGS};

    SYSTEM STOP MERGES t_parent_text_index;
    INSERT INTO t_parent_text_index VALUES (1, ('abc', 'def'));
    INSERT INTO t_parent_text_index VALUES (2, ('ghi', 'jkl'));
    SYSTEM START MERGES t_parent_text_index;
    OPTIMIZE TABLE t_parent_text_index FINAL;
    SELECT count() FROM t_parent_text_index;
    CHECK TABLE t_parent_text_index SETTINGS check_query_single_value_result = 1;
"
print_merge_algorithm t_parent_text_index

echo
echo '=== disabled statistics materialization does not pin ==='
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_no_materialize_parent_stats
    (
        k UInt64,
        t Tuple(x String, y String) STATISTICS(basic)
    )
    ENGINE = MergeTree
    ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        materialize_statistics_on_merge = 0;

    SYSTEM STOP MERGES t_no_materialize_parent_stats;
    INSERT INTO t_no_materialize_parent_stats SETTINGS materialize_statistics_on_insert = 1
        VALUES (1, ('a', 'b'));
    INSERT INTO t_no_materialize_parent_stats SETTINGS materialize_statistics_on_insert = 1
        VALUES (2, ('c', 'd'));
    SYSTEM START MERGES t_no_materialize_parent_stats;
    OPTIMIZE TABLE t_no_materialize_parent_stats FINAL;
    SELECT count() FROM t_no_materialize_parent_stats;
    CHECK TABLE t_no_materialize_parent_stats SETTINGS check_query_single_value_result = 1;
"
print_merge_algorithm t_no_materialize_parent_stats

echo
echo '=== persistent virtual-column indices do not pin ==='
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_virtual_indices
    (
        k UInt64,
        t Tuple(x String, y String)
    )
    ENGINE = MergeTree
    ORDER BY k
    SETTINGS
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        add_minmax_index_for_block_number_column = 1,
        add_minmax_index_for_block_offset_column = 1,
        vertical_merge_algorithm_min_rows_to_activate = 1,
        vertical_merge_algorithm_min_columns_to_activate = 4,
        allow_experimental_vertical_merge_tuple_subcolumns = 1,
        auto_statistics_types = '';

    SYSTEM STOP MERGES t_virtual_indices;
    INSERT INTO t_virtual_indices VALUES (1, ('a', 'b'));
    INSERT INTO t_virtual_indices VALUES (2, ('c', 'd'));
    SYSTEM START MERGES t_virtual_indices;
    OPTIMIZE TABLE t_virtual_indices FINAL;
    SELECT count() FROM t_virtual_indices;
    CHECK TABLE t_virtual_indices SETTINGS check_query_single_value_result = 1;
"
print_merge_algorithm t_virtual_indices
