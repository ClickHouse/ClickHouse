#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-object-storage, no-shared-merge-tree, no-replicated-database, no-parallel-replicas
#
# no-random-merge-tree-settings: pins flatten settings and Vertical activation.
# no-object-storage / no-shared-merge-tree: reads part files from a local directory.
#
# Mutations, patches, rename, TTL, projection, skip/text indexes, and lightweight
# delete against a flattened (or correctly unflattened) Tuple parent.

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

print_skip_index_files()
{
    local path="$1"
    find "$path" -maxdepth 1 -type f -printf '%f\n' | grep -E '^skp_idx' | sort
}

echo '=== text index on t.x flattens and rebuilds ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_text;
    CREATE TABLE t_text
    (
        k UInt64,
        t Tuple(s String, n UInt8),
        INDEX idx t.s TYPE text(tokenizer = ngrams(3)) GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        vertical_merge_tuple_subcolumns_fat_threshold_bytes = 1,
        index_granularity = 1;

    INSERT INTO t_text VALUES (1, ('hello', 1)), (2, ('world', 2));
    INSERT INTO t_text VALUES (3, ('hello', 3)), (4, ('foobar', 4));
    OPTIMIZE TABLE t_text FINAL;
    SELECT count() FROM t_text WHERE t.s = 'hello' SETTINGS force_data_skipping_indices = 'idx';
    SELECT k, t FROM t_text ORDER BY k;
    CHECK TABLE t_text SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_text;
"

echo
echo '=== ALTER UPDATE t then Vertical merge, including Compact patch ==='

${CLICKHOUSE_CLIENT} -q "
    SET enable_lightweight_update = 1;
    DROP TABLE IF EXISTS t_upd;
    CREATE TABLE t_upd
    (
        k UInt64,
        t Tuple(s String, n UInt8)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        vertical_merge_tuple_subcolumns_fat_threshold_bytes = 1,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        apply_patches_on_merge = 1;

    INSERT INTO t_upd VALUES (1, ('a', 1)), (2, ('b', 2));
    INSERT INTO t_upd VALUES (3, ('c', 3)), (4, ('d', 4));
    ALTER TABLE t_upd MODIFY SETTING min_rows_for_wide_part = 100000000, min_bytes_for_wide_part = 100000000;
    UPDATE t_upd SET t = ('upd', 99) WHERE k = 1;
    SELECT part_type FROM system.parts
    WHERE database = currentDatabase() AND table = 't_upd' AND active AND startsWith(name, 'patch')
    ORDER BY name;
    ALTER TABLE t_upd MODIFY SETTING min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
    OPTIMIZE TABLE t_upd FINAL;
    SELECT k, t FROM t_upd ORDER BY k;
    CHECK TABLE t_upd SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_upd;
"

echo
echo '=== DROP / MODIFY tuple element ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_drop;
    CREATE TABLE t_drop
    (
        k UInt64,
        t Tuple(x String, y String, z String)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        vertical_merge_tuple_subcolumns_fat_threshold_bytes = 1;

    INSERT INTO t_drop VALUES (1, ('a', 'b', 'c'));
    INSERT INTO t_drop VALUES (2, ('d', 'e', 'f'));
    ALTER TABLE t_drop MODIFY COLUMN t Tuple(x String, y String);
    INSERT INTO t_drop VALUES (3, ('g', 'h'));
    OPTIMIZE TABLE t_drop FINAL;
    SELECT k, t FROM t_drop ORDER BY k;
    CHECK TABLE t_drop SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_drop;
"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_mod;
    CREATE TABLE t_mod
    (
        k UInt64,
        t Tuple(x String, y String)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        vertical_merge_tuple_subcolumns_fat_threshold_bytes = 1;

    INSERT INTO t_mod VALUES (1, ('a', '10'));
    INSERT INTO t_mod VALUES (2, ('b', '20'));
    ALTER TABLE t_mod MODIFY COLUMN t Tuple(x String, y UInt64);
    INSERT INTO t_mod VALUES (3, ('c', 30));
    OPTIMIZE TABLE t_mod FINAL;
    SELECT k, t FROM t_mod ORDER BY k;
    CHECK TABLE t_mod SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_mod;
"

echo
echo '=== RENAME t to t2 then Vertical merge ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_ren;
    CREATE TABLE t_ren
    (
        k UInt64,
        t Tuple(s String, n UInt8)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        vertical_merge_tuple_subcolumns_fat_threshold_bytes = 1;

    INSERT INTO t_ren VALUES (1, ('a', 1));
    INSERT INTO t_ren VALUES (2, ('b', 2));
    ALTER TABLE t_ren RENAME COLUMN t TO t2;
    OPTIMIZE TABLE t_ren FINAL;
    SELECT k, t2 FROM t_ren ORDER BY k;
    SELECT k, t2.s, t2.n FROM t_ren ORDER BY k;
    CHECK TABLE t_ren SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_ren;
"

echo
echo '=== expired Tuple is not resurrected by leaf defaults ==='

${CLICKHOUSE_CLIENT} -q "
    SET allow_suspicious_ttl_expressions = 1;
    DROP TABLE IF EXISTS t_ttl;
    CREATE TABLE t_ttl
    (
        k UInt64,
        t Tuple(s String, n UInt8) TTL now() - INTERVAL 1 DAY
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        vertical_merge_tuple_subcolumns_fat_threshold_bytes = 1;

    INSERT INTO t_ttl VALUES (1, ('keep', 7));
    INSERT INTO t_ttl VALUES (2, ('gone', 8));
    OPTIMIZE TABLE t_ttl FINAL;
    SELECT k, t FROM t_ttl ORDER BY k;
    CHECK TABLE t_ttl SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_ttl;
"

echo
echo '=== projection on t pins t into merging_columns ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_proj;
    CREATE TABLE t_proj
    (
        k UInt64,
        t Tuple(s String, n UInt8),
        PROJECTION p (SELECT k, t ORDER BY k)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        vertical_merge_tuple_subcolumns_fat_threshold_bytes = 1;

    INSERT INTO t_proj VALUES (1, ('a', 1)), (2, ('b', 2));
    INSERT INTO t_proj VALUES (3, ('c', 3)), (4, ('d', 4));
    OPTIMIZE TABLE t_proj FINAL;
    SELECT k, t FROM t_proj ORDER BY k;
    SELECT count() > 0 FROM system.projection_parts
    WHERE database = currentDatabase() AND table = 't_proj' AND active;
    CHECK TABLE t_proj SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_proj;
"

echo
echo '=== index on t.x while ORDER BY t: rebuilt on horizontal writer ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_ord_stand;
    CREATE TABLE t_ord_stand
    (
        k UInt64,
        t Tuple(s String, n UInt8),
        INDEX idx t.s TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY t
    SETTINGS ${COMMON_SETTINGS},
        vertical_merge_tuple_subcolumns_fat_threshold_bytes = 1,
        index_granularity = 1,
        packed_skip_index_max_bytes = 0;

    INSERT INTO t_ord_stand VALUES (1, ('a', 1)), (2, ('b', 2));
    INSERT INTO t_ord_stand VALUES (3, ('c', 3)), (4, ('d', 4));
    OPTIMIZE TABLE t_ord_stand FINAL;
    SELECT count() FROM t_ord_stand WHERE t.s = 'c' SETTINGS force_data_skipping_indices = 'idx';
    CHECK TABLE t_ord_stand SETTINGS check_query_single_value_result = 1;
"

echo 'standalone_skip_index_files'
print_skip_index_files "$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_ord_stand' AND active")"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ord_stand;"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_ord_pack;
    CREATE TABLE t_ord_pack
    (
        k UInt64,
        t Tuple(s String, n UInt8),
        INDEX idx t.s TYPE minmax GRANULARITY 1
    )
    ENGINE = MergeTree ORDER BY t
    SETTINGS ${COMMON_SETTINGS},
        vertical_merge_tuple_subcolumns_fat_threshold_bytes = 1,
        index_granularity = 1,
        packed_skip_index_max_bytes = 1048576;

    INSERT INTO t_ord_pack VALUES (1, ('a', 1)), (2, ('b', 2));
    INSERT INTO t_ord_pack VALUES (3, ('c', 3)), (4, ('d', 4));
    OPTIMIZE TABLE t_ord_pack FINAL;
    SELECT count() FROM t_ord_pack WHERE t.s = 'c' SETTINGS force_data_skipping_indices = 'idx';
    CHECK TABLE t_ord_pack SETTINGS check_query_single_value_result = 1;
"

echo 'packed_skip_index_files'
print_skip_index_files "$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_ord_pack' AND active")"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_ord_pack;"

echo
echo '=== lightweight delete + Vertical ==='

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_lwd;
    CREATE TABLE t_lwd
    (
        k UInt64,
        t Tuple(s String, n UInt8)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS ${COMMON_SETTINGS},
        vertical_merge_tuple_subcolumns_fat_threshold_bytes = 1,
        vertical_merge_optimize_lightweight_delete = 1;

    INSERT INTO t_lwd VALUES (1, ('a', 1)), (2, ('b', 2)), (3, ('c', 3));
    INSERT INTO t_lwd VALUES (4, ('d', 4)), (5, ('e', 5)), (6, ('f', 6));
    DELETE FROM t_lwd WHERE k % 2 = 0;
    SELECT k, t FROM t_lwd ORDER BY k;
    OPTIMIZE TABLE t_lwd FINAL;
    SELECT k, t FROM t_lwd ORDER BY k;
    CHECK TABLE t_lwd SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_lwd;
"
