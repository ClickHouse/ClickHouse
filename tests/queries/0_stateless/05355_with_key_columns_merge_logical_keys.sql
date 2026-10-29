-- Tags: no-random-settings, no-random-merge-tree-settings
SET allow_experimental_map_key_columns = 1;

-- Key union for with_key_columns must follow the rows the merge will write:
-- a patch can introduce a key the source manifest does not have, a lightweight
-- delete can be the only row that has a key, and a part from before ADD COLUMN
-- does not store the Map column at all.

SET optimize_on_insert = 0;
SET mutations_sync = 2;
SET lightweight_deletes_sync = 2;
SET enable_lightweight_update = 1;

SELECT 'add_column';
DROP TABLE IF EXISTS t_wkc_add;
CREATE TABLE t_wkc_add
(
    id UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    map_key_columns_per_key_merge_min_keys = 1,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

SYSTEM STOP MERGES t_wkc_add;
INSERT INTO t_wkc_add VALUES (1);
ALTER TABLE t_wkc_add ADD COLUMN m Map(String, UInt64);
SELECT count()
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_wkc_add' AND active AND column = 'm';
INSERT INTO t_wkc_add VALUES (2, map('a', 1));
SELECT count()
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_wkc_add' AND active AND column = 'm';
SYSTEM START MERGES t_wkc_add;
OPTIMIZE TABLE t_wkc_add FINAL;

SELECT id, mapSort(m), length(m)
FROM t_wkc_add
ORDER BY id;

CHECK TABLE t_wkc_add SETTINGS check_query_single_value_result = 1;

SELECT 'patch_key';
DROP TABLE IF EXISTS t_wkc_patch;
CREATE TABLE t_wkc_patch
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    map_key_columns_per_key_merge_min_keys = 2,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

SYSTEM STOP MERGES t_wkc_patch;
INSERT INTO t_wkc_patch VALUES (1, map('a', 1, 'b', 2));
INSERT INTO t_wkc_patch VALUES (2, map('c', 3, 'd', 4));
UPDATE t_wkc_patch SET m = map('a', 1, 'b', 2, 'z', 9) WHERE id = 1;
SYSTEM START MERGES t_wkc_patch;
OPTIMIZE TABLE t_wkc_patch FINAL;

SELECT id, mapSort(m), m['z'], length(m)
FROM t_wkc_patch
ORDER BY id
SETTINGS apply_patch_parts = 0;

CHECK TABLE t_wkc_patch SETTINGS check_query_single_value_result = 1;

SELECT 'deleted_key';
DROP TABLE IF EXISTS t_wkc_del;
CREATE TABLE t_wkc_del
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
    max_map_key_columns_keys = 4,
    map_key_columns_per_key_merge_min_keys = 4,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

SYSTEM STOP MERGES t_wkc_del;
INSERT INTO t_wkc_del VALUES (1, map('a', 1, 'b', 2, 'c', 3, 'd', 4));
INSERT INTO t_wkc_del VALUES (2, map('extra', 1));
-- Merges are stopped, so do not wait for the delete mutation here. OPTIMIZE applies it.
SET mutations_sync = 0;
SET lightweight_deletes_sync = 0;
DELETE FROM t_wkc_del WHERE id = 2;
SET mutations_sync = 2;
SET lightweight_deletes_sync = 2;
SYSTEM START MERGES t_wkc_del;
OPTIMIZE TABLE t_wkc_del FINAL;

SELECT id, mapSort(m), length(m)
FROM t_wkc_del
ORDER BY id
SETTINGS apply_patch_parts = 0;

CHECK TABLE t_wkc_del SETTINGS check_query_single_value_result = 1;

SELECT 'hashed_name';
DROP TABLE IF EXISTS t_wkc_name;
CREATE TABLE t_wkc_name
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
    replace_long_file_name_to_hash = 1,
    max_file_name_length = 1,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

INSERT INTO t_wkc_name VALUES (1, map('k', 7));
ALTER TABLE t_wkc_name MODIFY SETTING replace_long_file_name_to_hash = 0, max_file_name_length = 10000;
DETACH TABLE t_wkc_name;
ATTACH TABLE t_wkc_name;

SELECT id, m['k'], length(m)
FROM t_wkc_name
ORDER BY id;

CHECK TABLE t_wkc_name SETTINGS check_query_single_value_result = 1;

DROP TABLE t_wkc_add;
DROP TABLE t_wkc_patch;
DROP TABLE t_wkc_del;
DROP TABLE t_wkc_name;
