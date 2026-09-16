#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-replicated-database, no-parallel-replicas
#
# no-random-merge-tree-settings: pins flatten settings and Vertical activation.
#
# Refuse-to-flatten gates for vertical Tuple-subcolumn merge. Merge, `CHECK`,
# and reads must still succeed when a parent stays one `StorageColumn`.

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
    ratio_of_defaults_for_sparse_serialization = 0.9,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    allow_experimental_vertical_merge_tuple_subcolumns = 1,
    auto_statistics_types = ''
"

echo '=== Nullable(Tuple), Point, empty Tuple are not flattened ==='

${CLICKHOUSE_CLIENT} -q "
    SET enable_nullable_tuple_type = 1;
    DROP TABLE IF EXISTS t_nullable;
    CREATE TABLE t_nullable
    (
        k UInt64,
        t Nullable(Tuple(x String, y String))
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS};

    INSERT INTO t_nullable VALUES (1, ('a', 'b')), (2, NULL);
    INSERT INTO t_nullable VALUES (3, ('c', 'd')), (4, NULL);
    OPTIMIZE TABLE t_nullable FINAL;
    SELECT k, t FROM t_nullable ORDER BY k;
    CHECK TABLE t_nullable SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_nullable;
"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_point;
    CREATE TABLE t_point
    (
        k UInt64,
        t Point
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS};

    INSERT INTO t_point VALUES (1, (1.5, 2.5));
    INSERT INTO t_point VALUES (2, (3.5, 4.5));
    OPTIMIZE TABLE t_point FINAL;
    SELECT k, t FROM t_point ORDER BY k;
    CHECK TABLE t_point SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_point;
"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_empty;
    CREATE TABLE t_empty
    (
        k UInt64,
        t Tuple()
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS};

    INSERT INTO t_empty VALUES (1, ()), (2, ());
    INSERT INTO t_empty VALUES (3, ());
    OPTIMIZE TABLE t_empty FINAL;
    SELECT k, t FROM t_empty ORDER BY k;
    CHECK TABLE t_empty SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_empty;
"

echo
echo '=== leaf name collision with physical column t.x is rejected at CREATE ==='

# A physical column `t.x` and Tuple leaf `t.x` share the stream `t%2Ex`, so the
# table cannot be created. Classify still has `leafNameCollides` as a defensive
# gate if such a schema ever reaches a merge.
set +e
coll_err=$(${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_coll
    (
        k UInt64,
        t Tuple(x String, y String),
        \`t.x\` String
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS}
" 2>&1)
set -e
if echo "$coll_err" | grep -q 'collision in file name'; then echo 1; else echo 0; fi

echo
echo '=== skip index on whole t pins the parent ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_idx_parent;
    CREATE TABLE t_idx_parent
    (
        k UInt64,
        t Tuple(x String, y String),
        INDEX idx cityHash64(t) TYPE bloom_filter(0.01) GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        index_granularity = 1;

    INSERT INTO t_idx_parent VALUES (1, ('a', 'b')), (2, ('c', 'd'));
    INSERT INTO t_idx_parent VALUES (3, ('e', 'f')), (4, ('g', 'h'));
    OPTIMIZE TABLE t_idx_parent FINAL;
    SELECT k, t FROM t_idx_parent ORDER BY k;
    SELECT count() FROM t_idx_parent WHERE cityHash64(t) = cityHash64(('c', 'd')) SETTINGS force_data_skipping_indices = 'idx';
    CHECK TABLE t_idx_parent SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_idx_parent;
"
