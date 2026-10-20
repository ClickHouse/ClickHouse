-- Tags: no-random-settings, no-random-merge-tree-settings

-- An empty loaded `with_key_columns` manifest is an empty key set: merging it
-- with a part that has keys must not invent keys for the empty rows.
-- A `basic` source part is not authoritative and is still scanned into the union.

SET optimize_on_insert = 0;

SELECT 'empty_manifest';
DROP TABLE IF EXISTS t_wkc_empty_manifest;
CREATE TABLE t_wkc_empty_manifest
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
    map_key_columns_per_key_merge_min_keys = 100,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

SYSTEM STOP MERGES t_wkc_empty_manifest;
INSERT INTO t_wkc_empty_manifest VALUES (1, map()), (2, map());
INSERT INTO t_wkc_empty_manifest VALUES (3, map('k', 4));
SYSTEM START MERGES t_wkc_empty_manifest;
OPTIMIZE TABLE t_wkc_empty_manifest FINAL;

SELECT
    id,
    mapKeys(m),
    mapContains(m, 'k'),
    m['k']
FROM t_wkc_empty_manifest
ORDER BY id;

CHECK TABLE t_wkc_empty_manifest SETTINGS check_query_single_value_result = 1;

SELECT 'basic_source';
DROP TABLE IF EXISTS t_wkc_basic_source;
CREATE TABLE t_wkc_basic_source
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'basic',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    map_key_columns_per_key_merge_min_keys = 100,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

SYSTEM STOP MERGES t_wkc_basic_source;
INSERT INTO t_wkc_basic_source VALUES (1, map('b', 5));
ALTER TABLE t_wkc_basic_source MODIFY SETTING map_serialization_version_for_zero_level_parts = 'with_key_columns';
INSERT INTO t_wkc_basic_source VALUES (2, map('a', 7, 'b', 8));
SYSTEM START MERGES t_wkc_basic_source;
OPTIMIZE TABLE t_wkc_basic_source FINAL;

SELECT
    id,
    mapKeys(m),
    m['a'],
    m['b'],
    mapContains(m, 'a'),
    mapContains(m, 'b')
FROM t_wkc_basic_source
ORDER BY id;

CHECK TABLE t_wkc_basic_source SETTINGS check_query_single_value_result = 1;

DROP TABLE t_wkc_empty_manifest;
DROP TABLE t_wkc_basic_source;
