-- Tags: no-random-settings, no-random-merge-tree-settings

-- Per-key Vertical merge for with_key_columns Map: union keys across parts,
-- missing keys stay absent, Compact sources rewrite to Wide, and DEDUPLICATE
-- fail-closes when the key union meets the threshold.

SET optimize_on_insert = 0;

SELECT 'small_k_union';
DROP TABLE IF EXISTS t_wkc_small;
CREATE TABLE t_wkc_small
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    map_key_columns_per_key_merge_min_keys = 32,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

SYSTEM STOP MERGES t_wkc_small;
INSERT INTO t_wkc_small VALUES (1, map('a', 1, 'b', 2));
INSERT INTO t_wkc_small VALUES (2, map('b', 3, 'c', 4));
SYSTEM START MERGES t_wkc_small;
OPTIMIZE TABLE t_wkc_small FINAL;

SELECT
    id,
    mapSort(m),
    m['a'],
    m['b'],
    m['c'],
    m.exists_a,
    m.exists_b,
    m.exists_c
FROM t_wkc_small
ORDER BY id;

CHECK TABLE t_wkc_small SETTINGS check_query_single_value_result = 1;

SELECT 'large_k_per_key';
DROP TABLE IF EXISTS t_wkc_large;
CREATE TABLE t_wkc_large
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    map_key_columns_per_key_merge_min_keys = 4,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

SYSTEM STOP MERGES t_wkc_large;
INSERT INTO t_wkc_large VALUES (1, map('a', 1, 'b', 2, 'c', 3, 'd', 4));
INSERT INTO t_wkc_large VALUES (2, map('c', 5, 'd', 6, 'e', 7, 'f', 8));
SYSTEM START MERGES t_wkc_large;
OPTIMIZE TABLE t_wkc_large FINAL;

SELECT
    id,
    mapSort(m),
    m['a'],
    m['b'],
    m['c'],
    m['d'],
    m['e'],
    m['f'],
    m.exists_a,
    m.exists_b,
    m.exists_c,
    m.exists_d,
    m.exists_e,
    m.exists_f
FROM t_wkc_large
ORDER BY id;

CHECK TABLE t_wkc_large SETTINGS check_query_single_value_result = 1;

SELECT 'compact_union';
DROP TABLE IF EXISTS t_wkc_compact;
CREATE TABLE t_wkc_compact
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000,
    map_key_columns_per_key_merge_min_keys = 3,
    allow_vertical_merges_from_compact_to_wide_parts = 1,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

SYSTEM STOP MERGES t_wkc_compact;
INSERT INTO t_wkc_compact VALUES (1, map('a', 1, 'b', 2, 'c', 0));
INSERT INTO t_wkc_compact VALUES (2, map('c', 3, 'd', 4, 'e', 5));
SYSTEM START MERGES t_wkc_compact;
OPTIMIZE TABLE t_wkc_compact FINAL;

SELECT DISTINCT part_type
FROM system.parts
WHERE database = currentDatabase() AND table = 't_wkc_compact' AND active;

SELECT
    id,
    mapSort(m),
    m['a'],
    m['d'],
    m.exists_a,
    m.exists_d
FROM t_wkc_compact
ORDER BY id;

CHECK TABLE t_wkc_compact SETTINGS check_query_single_value_result = 1;

SELECT 'complex_values';
DROP TABLE IF EXISTS t_wkc_complex;
CREATE TABLE t_wkc_complex
(
    id UInt64,
    m Map(String, String)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    map_key_columns_per_key_merge_min_keys = 3,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

SYSTEM STOP MERGES t_wkc_complex;
INSERT INTO t_wkc_complex VALUES (1, map('a', 'xy', 'b', 'z'));
INSERT INTO t_wkc_complex VALUES (2, map('b', 'w', 'c', 'uv'));
SYSTEM START MERGES t_wkc_complex;
OPTIMIZE TABLE t_wkc_complex FINAL;

SELECT
    id,
    mapSort(m),
    m['a'],
    m['b'],
    m['c'],
    m.exists_a,
    m.exists_c
FROM t_wkc_complex
ORDER BY id;

CHECK TABLE t_wkc_complex SETTINGS check_query_single_value_result = 1;

SELECT 'deduplicate_fails';
DROP TABLE IF EXISTS t_wkc_dedup;
CREATE TABLE t_wkc_dedup
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    map_key_columns_per_key_merge_min_keys = 4,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

SYSTEM STOP MERGES t_wkc_dedup;
INSERT INTO t_wkc_dedup VALUES (1, map('a', 1, 'b', 2, 'c', 3, 'd', 4));
INSERT INTO t_wkc_dedup VALUES (2, map('a', 5, 'b', 6, 'c', 7, 'd', 8));
SYSTEM START MERGES t_wkc_dedup;
OPTIMIZE TABLE t_wkc_dedup DEDUPLICATE; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_wkc_small;
DROP TABLE t_wkc_large;
DROP TABLE t_wkc_compact;
DROP TABLE t_wkc_complex;
DROP TABLE t_wkc_dedup;
