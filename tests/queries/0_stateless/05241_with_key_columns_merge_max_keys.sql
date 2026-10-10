-- Tags: no-random-settings, no-random-merge-tree-settings

-- `map_max_key_columns` applies to both the full-column merge and the per-key
-- merge. A non-zero limit smaller than the union fails with `LIMIT_EXCEEDED`
-- and publishes no merged part. Zero means unlimited and the same data merges.

SET optimize_on_insert = 0;

DROP TABLE IF EXISTS t_wkc_max_full;
DROP TABLE IF EXISTS t_wkc_max_per_key;

CREATE TABLE t_wkc_max_full
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
    map_max_key_columns = 1,
    map_key_columns_per_key_merge_min_keys = 100,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

CREATE TABLE t_wkc_max_per_key
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
    map_max_key_columns = 1,
    map_key_columns_per_key_merge_min_keys = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    add_minmax_index_for_numeric_columns = 0,
    auto_statistics_types = '';

SYSTEM STOP MERGES t_wkc_max_full;
SYSTEM STOP MERGES t_wkc_max_per_key;
INSERT INTO t_wkc_max_full VALUES (1, map('b', 1));
INSERT INTO t_wkc_max_full VALUES (2, map('a', 2));
INSERT INTO t_wkc_max_per_key VALUES (1, map('b', 1));
INSERT INTO t_wkc_max_per_key VALUES (2, map('a', 2));
SYSTEM START MERGES t_wkc_max_full;
SYSTEM START MERGES t_wkc_max_per_key;

OPTIMIZE TABLE t_wkc_max_full FINAL; -- { serverError LIMIT_EXCEEDED }
OPTIMIZE TABLE t_wkc_max_per_key FINAL; -- { serverError LIMIT_EXCEEDED }

SELECT 'parts_after_limit';
SELECT
    count()
FROM system.parts
WHERE database = currentDatabase() AND table = 't_wkc_max_full' AND active;
SELECT
    count()
FROM system.parts
WHERE database = currentDatabase() AND table = 't_wkc_max_per_key' AND active;

ALTER TABLE t_wkc_max_full MODIFY SETTING map_max_key_columns = 0;
ALTER TABLE t_wkc_max_per_key MODIFY SETTING map_max_key_columns = 0;

OPTIMIZE TABLE t_wkc_max_full FINAL;
OPTIMIZE TABLE t_wkc_max_per_key FINAL;

SELECT 'merged';
SELECT
    id,
    mapKeys(m),
    m['a'],
    m['b'],
    mapContains(m, 'a'),
    mapContains(m, 'b')
FROM t_wkc_max_full
ORDER BY id;

SELECT count()
FROM
(
    SELECT id, mapKeys(m), m['a'], m['b'], mapContains(m, 'a'), mapContains(m, 'b')
    FROM t_wkc_max_full
    EXCEPT ALL
    SELECT id, mapKeys(m), m['a'], m['b'], mapContains(m, 'a'), mapContains(m, 'b')
    FROM t_wkc_max_per_key
);

CHECK TABLE t_wkc_max_full SETTINGS check_query_single_value_result = 1;
CHECK TABLE t_wkc_max_per_key SETTINGS check_query_single_value_result = 1;

SYSTEM FLUSH LOGS part_log;
SELECT 'algorithms';
SELECT merge_algorithm
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_wkc_max_full' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC
LIMIT 1;
SELECT merge_algorithm
FROM system.part_log
WHERE database = currentDatabase() AND table = 't_wkc_max_per_key' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_wkc_max_full;
DROP TABLE t_wkc_max_per_key;
