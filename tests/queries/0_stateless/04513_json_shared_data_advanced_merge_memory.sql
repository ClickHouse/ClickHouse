-- Tags: long, no-debug, no-asan, no-tsan, no-msan, no-random-settings, no-random-merge-tree-settings
-- Repro for https://github.com/ClickHouse/ClickHouse/issues/108992:
-- merges of a JSON column with `object_shared_data_serialization_version = 'advanced'`
-- materialize all shared data paths in memory at once (peak ~ unique_paths x rows),
-- while the `map` serialization streams them. Peak merge memory must stay within a
-- small factor of the `map` baseline instead of scaling with the merged data size.

SET enable_json_type = 1;

DROP TABLE IF EXISTS t_shared_data_merge_map;
DROP TABLE IF EXISTS t_shared_data_merge_advanced;

CREATE TABLE t_shared_data_merge_map (id UInt64, data JSON(max_dynamic_paths = 2))
ENGINE = MergeTree ORDER BY id
SETTINGS
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'map',
    object_shared_data_serialization_version_for_zero_level_parts = 'map',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

CREATE TABLE t_shared_data_merge_advanced (id UInt64, data JSON(max_dynamic_paths = 2))
ENGINE = MergeTree ORDER BY id
SETTINGS
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_zero_level_parts = 'map_with_buckets',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_shared_data_merge_map;
SYSTEM STOP MERGES t_shared_data_merge_advanced;

-- Two parts per table, ~1000 unique sparse paths overflowing into shared data
-- (max_dynamic_paths = 2 keeps them out of dynamic subcolumns).
INSERT INTO t_shared_data_merge_map
SELECT number, toJSONString(map(
    'sparse_' || toString(number % 1000), repeat(toString(number), 5),
    'common', toString(number)))
FROM numbers(150000);

INSERT INTO t_shared_data_merge_map
SELECT number, toJSONString(map(
    'sparse_' || toString(number % 1000), repeat(toString(number), 5),
    'common', toString(number)))
FROM numbers(150000, 150000);

INSERT INTO t_shared_data_merge_advanced
SELECT number, toJSONString(map(
    'sparse_' || toString(number % 1000), repeat(toString(number), 5),
    'common', toString(number)))
FROM numbers(150000);

INSERT INTO t_shared_data_merge_advanced
SELECT number, toJSONString(map(
    'sparse_' || toString(number % 1000), repeat(toString(number), 5),
    'common', toString(number)))
FROM numbers(150000, 150000);

SYSTEM START MERGES t_shared_data_merge_map;
SYSTEM START MERGES t_shared_data_merge_advanced;

OPTIMIZE TABLE t_shared_data_merge_map FINAL SETTINGS optimize_throw_if_noop = 1;
OPTIMIZE TABLE t_shared_data_merge_advanced FINAL SETTINGS optimize_throw_if_noop = 1;

-- Sanity: both tables fully merged with identical content.
SELECT count(), uniqExact(data.common) FROM t_shared_data_merge_map;
SELECT count(), uniqExact(data.common) FROM t_shared_data_merge_advanced;

SYSTEM FLUSH LOGS part_log;

-- The `advanced` merge must not use disproportionally more memory than the `map` one.
WITH
    (
        SELECT max(peak_memory_usage)
        FROM system.part_log
        WHERE database = currentDatabase() AND table = 't_shared_data_merge_map'
            AND event_type = 'MergeParts' AND error = 0
    ) AS peak_map,
    (
        SELECT max(peak_memory_usage)
        FROM system.part_log
        WHERE database = currentDatabase() AND table = 't_shared_data_merge_advanced'
            AND event_type = 'MergeParts' AND error = 0
    ) AS peak_advanced
SELECT
    peak_map > 0,
    peak_advanced < 4 * peak_map;

DROP TABLE t_shared_data_merge_map;
DROP TABLE t_shared_data_merge_advanced;
