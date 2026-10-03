-- Tags: no-random-settings, no-random-merge-tree-settings

-- Full-column merge must register the key union before the first output block.
-- `z` and `a` exist only in the second granule of the second part, and
-- `merge_max_block_size` splits the output so those keys are absent from the
-- first block's data. Prefix rows stay missing, later values stay intact, and
-- `mapKeys` follows Field order (`a`, `m`, `z`), not first-seen order.
-- A `LowCardinality` value merges the same way: the key is registered before
-- the part has history, so the insert-only template copy is not used.

SET optimize_on_insert = 0;

SELECT 'late_key';
DROP TABLE IF EXISTS t_wkc_late;
CREATE TABLE t_wkc_late
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
    index_granularity = 1,
    index_granularity_bytes = 0,
    merge_max_block_size = 1,
    map_key_columns_per_key_merge_min_keys = 100,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

SYSTEM STOP MERGES t_wkc_late;
INSERT INTO t_wkc_late VALUES (1, map('m', 10));
INSERT INTO t_wkc_late VALUES (2, map('m', 20)), (3, map('a', 1, 'm', 30, 'z', 9));
SYSTEM START MERGES t_wkc_late;
OPTIMIZE TABLE t_wkc_late FINAL;

SELECT part_type
FROM system.parts
WHERE database = currentDatabase() AND table = 't_wkc_late' AND active;

SELECT
    id,
    mapKeys(m),
    m['a'],
    m['m'],
    m['z'],
    mapContains(m, 'a'),
    mapContains(m, 'z')
FROM t_wkc_late
ORDER BY id;

CHECK TABLE t_wkc_late SETTINGS check_query_single_value_result = 1;

SYSTEM FLUSH LOGS part_log;
SELECT merge_algorithm
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_wkc_late' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT 'lc_merge';
DROP TABLE IF EXISTS t_wkc_late_lc;
CREATE TABLE t_wkc_late_lc
(
    id UInt64,
    m Map(String, LowCardinality(String))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    index_granularity = 1,
    index_granularity_bytes = 0,
    merge_max_block_size = 1,
    map_key_columns_per_key_merge_min_keys = 100,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

SYSTEM STOP MERGES t_wkc_late_lc;
INSERT INTO t_wkc_late_lc VALUES (1, map('m', 'M1'));
INSERT INTO t_wkc_late_lc VALUES (2, map('m', 'M2')), (3, map('a', 'A', 'm', 'M3', 'z', 'Z'));
SYSTEM START MERGES t_wkc_late_lc;
OPTIMIZE TABLE t_wkc_late_lc FINAL;

SELECT
    id,
    mapKeys(m),
    m['a'],
    m['m'],
    m['z'],
    mapContains(m, 'a'),
    mapContains(m, 'z')
FROM t_wkc_late_lc
ORDER BY id;

CHECK TABLE t_wkc_late_lc SETTINGS check_query_single_value_result = 1;

DROP TABLE t_wkc_late;
DROP TABLE t_wkc_late_lc;
