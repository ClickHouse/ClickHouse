#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-object-storage, no-shared-merge-tree, no-replicated-database, no-parallel-replicas
#
# no-random-merge-tree-settings: pins flatten settings and Vertical activation.
# no-object-storage / no-shared-merge-tree: reads part files from a local directory.
#
# Refuse-to-flatten gates for vertical Tuple-subcolumn merge. Cases that would
# flatten (low Fat threshold or a fat `String`) stay one `StorageColumn` when a
# gate fires. Merge, `CHECK`, and reads must still succeed.

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
    allow_experimental_vertical_merge_tuple_subcolumns = 1
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
    SETTINGS ${COMMON_SETTINGS},
        vertical_merge_tuple_subcolumns_fat_threshold_bytes = 1;

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
    SETTINGS ${COMMON_SETTINGS},
        vertical_merge_tuple_subcolumns_fat_threshold_bytes = 1;

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
    SETTINGS ${COMMON_SETTINGS},
        vertical_merge_tuple_subcolumns_fat_threshold_bytes = 1;

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
    SETTINGS ${COMMON_SETTINGS},
        vertical_merge_tuple_subcolumns_fat_threshold_bytes = 1
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
        vertical_merge_tuple_subcolumns_fat_threshold_bytes = 1,
        index_granularity = 1;

    INSERT INTO t_idx_parent VALUES (1, ('a', 'b')), (2, ('c', 'd'));
    INSERT INTO t_idx_parent VALUES (3, ('e', 'f')), (4, ('g', 'h'));
    OPTIMIZE TABLE t_idx_parent FINAL;
    SELECT k, t FROM t_idx_parent ORDER BY k;
    SELECT count() FROM t_idx_parent WHERE cityHash64(t) = cityHash64(('c', 'd')) SETTINGS force_data_skipping_indices = 'idx';
    CHECK TABLE t_idx_parent SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_idx_parent;
"

echo
echo '=== Tuple(UInt8 x 20) is not flattened ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_u8;
    CREATE TABLE t_u8
    (
        k UInt64,
        t Tuple(
            c0 UInt8, c1 UInt8, c2 UInt8, c3 UInt8, c4 UInt8,
            c5 UInt8, c6 UInt8, c7 UInt8, c8 UInt8, c9 UInt8,
            c10 UInt8, c11 UInt8, c12 UInt8, c13 UInt8, c14 UInt8,
            c15 UInt8, c16 UInt8, c17 UInt8, c18 UInt8, c19 UInt8)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS};

    INSERT INTO t_u8 SELECT
        number,
        (number, number + 1, number + 2, number + 3, number + 4,
         number + 5, number + 6, number + 7, number + 8, number + 9,
         number + 10, number + 11, number + 12, number + 13, number + 14,
         number + 15, number + 16, number + 17, number + 18, number + 19)
    FROM numbers(16);
    INSERT INTO t_u8 SELECT
        number + 16,
        (number, number + 1, number + 2, number + 3, number + 4,
         number + 5, number + 6, number + 7, number + 8, number + 9,
         number + 10, number + 11, number + 12, number + 13, number + 14,
         number + 15, number + 16, number + 17, number + 18, number + 19)
    FROM numbers(16);
    OPTIMIZE TABLE t_u8 FINAL;
    SELECT count(), sum(t.c0), sum(t.c19) FROM t_u8;
    CHECK TABLE t_u8 SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_u8;
"

echo
echo '=== near-Fat tinies whose granule sum >= Fat threshold ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_near;
    CREATE TABLE t_near
    (
        k UInt64,
        t Tuple(
            s String,
            c0 UInt8, c1 UInt8, c2 UInt8, c3 UInt8, c4 UInt8,
            c5 UInt8, c6 UInt8, c7 UInt8, c8 UInt8, c9 UInt8,
            c10 UInt8, c11 UInt8, c12 UInt8, c13 UInt8, c14 UInt8,
            c15 UInt8, c16 UInt8, c17 UInt8, c18 UInt8, c19 UInt8)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        vertical_merge_tuple_subcolumns_fat_threshold_bytes = 5000,
        index_granularity = 8192;

    INSERT INTO t_near SELECT
        number,
        (repeat('x', 100),
         0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
    FROM numbers(1000);
    INSERT INTO t_near SELECT
        number + 1000,
        (repeat('y', 100),
         0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
    FROM numbers(1000);
    OPTIMIZE TABLE t_near FINAL;
    SELECT count(), sum(length(t.s)), sum(t.c0), sum(t.c19) FROM t_near;
    CHECK TABLE t_near SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_near;
"

echo
echo '=== merge_max_block_size_bytes=0 does not refuse or force flatten ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_mb0_v;
    DROP TABLE IF EXISTS t_mb0_h;
    CREATE TABLE t_mb0_v
    (
        k UInt64,
        t Tuple(s String, n UInt8)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        merge_max_block_size_bytes = 0,
        index_granularity_bytes = 0;

    CREATE TABLE t_mb0_h AS t_mb0_v
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        merge_max_block_size_bytes = 0,
        index_granularity_bytes = 0,
        enable_vertical_merge_algorithm = 0;

    INSERT INTO t_mb0_v SELECT number, (repeat('x', 12000), toUInt8(number)) FROM numbers(1000);
    INSERT INTO t_mb0_v SELECT number + 1000, (repeat('y', 12000), toUInt8(number)) FROM numbers(1000);
    INSERT INTO t_mb0_h SELECT number, (repeat('x', 12000), toUInt8(number)) FROM numbers(1000);
    INSERT INTO t_mb0_h SELECT number + 1000, (repeat('y', 12000), toUInt8(number)) FROM numbers(1000);
    OPTIMIZE TABLE t_mb0_v FINAL;
    OPTIMIZE TABLE t_mb0_h FINAL;
"

echo 'select_match'
${CLICKHOUSE_CLIENT} -q "
    SELECT count() = 0 FROM
    (
        SELECT * FROM t_mb0_v
        EXCEPT
        SELECT * FROM t_mb0_h
    )
"
echo 'check'
${CLICKHOUSE_CLIENT} -q "CHECK TABLE t_mb0_v SETTINGS check_query_single_value_result = 1"

V_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_mb0_v' AND active")
echo 'serialization_keys'
python3 - "$V_PATH" <<'PY'
import json, os, sys
path = os.path.join(sys.argv[1], "serialization.json")
if not os.path.exists(path):
    print("missing")
    raise SystemExit(0)
with open(path) as f:
    data = json.load(f)
cols = data.get("columns", [])
print(*[c.get("name") for c in cols])
for col in cols:
    if col.get("name") == "t":
        print("t_exact", col.get("exact_num_defaults", False))
PY

echo 'merge_algorithm'
print_merge_algorithm t_mb0_v
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_mb0_v; DROP TABLE t_mb0_h;"

echo
echo '=== one fat Tuple does not activate Vertical ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_novert;
    CREATE TABLE t_novert
    (
        k UInt64,
        t Tuple(s String, n UInt8)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0,
        min_bytes_for_full_part_storage = 0,
        enable_block_number_column = 0,
        enable_block_offset_column = 0,
        vertical_merge_algorithm_min_rows_to_activate = 1,
        allow_experimental_vertical_merge_tuple_subcolumns = 1,
        vertical_merge_tuple_subcolumns_fat_threshold_bytes = 1;

    INSERT INTO t_novert VALUES (1, ('a', 1));
    INSERT INTO t_novert VALUES (2, ('b', 2));
    OPTIMIZE TABLE t_novert FINAL;
    SELECT k, t FROM t_novert ORDER BY k;
    CHECK TABLE t_novert SETTINGS check_query_single_value_result = 1;
"

echo 'merge_algorithm'
print_merge_algorithm t_novert
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_novert;"
