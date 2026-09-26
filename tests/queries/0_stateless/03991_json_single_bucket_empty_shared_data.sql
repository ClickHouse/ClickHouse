-- Test: SerializationObject creates a single bucket for shared data when shared data is empty.
-- When all JSON paths fit into typed/dynamic paths, shared_data_paths_statistics is empty,
-- and the system should use 1 bucket instead of the configured number for Wide parts.
-- Requires no-random-merge-tree-settings because the test validates exact bucket counts
-- in system.parts_columns, which depend on specific serialization version settings.

SET allow_experimental_json_type = 1;

-- ==========================================
-- Section 1: Wide part with empty shared data - single bucket
-- When max_dynamic_paths is large enough to hold all paths, shared data is empty.
-- ==========================================

DROP TABLE IF EXISTS t_json_single_bucket;

CREATE TABLE t_json_single_bucket (id UInt64, j JSON(max_dynamic_paths = 100))
ENGINE = MergeTree ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
    object_shared_data_buckets_for_wide_part = 32,
    object_shared_data_buckets_for_compact_part = 8;

-- Only 2 distinct paths, well within max_dynamic_paths. Shared data stays empty.
INSERT INTO t_json_single_bucket SELECT number, '{"a": ' || toString(number) || ', "b": ' || toString(number + 1) || '}' FROM numbers(100);

SELECT '-- Wide part, empty shared data: single bucket expected';
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_json_single_bucket' AND active = 1 LIMIT 1;

-- Count the number of bucket substreams. Each bucket has a '.structure' substream.
SELECT
    length(arrayFilter(x -> x LIKE 'j.object\_shared\_data.%.data', substreams)) AS shared_data_bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_json_single_bucket' AND column = 'j' AND active = 1
LIMIT 1;

DROP TABLE t_json_single_bucket;

-- ==========================================
-- Section 2: Wide part with non-empty shared data - multiple buckets
-- When max_dynamic_paths is small, excess paths go to shared data.
-- ==========================================

DROP TABLE IF EXISTS t_json_multi_bucket;

CREATE TABLE t_json_multi_bucket (id UInt64, j JSON(max_dynamic_paths = 2))
ENGINE = MergeTree ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
    object_shared_data_buckets_for_wide_part = 4,
    object_shared_data_buckets_for_compact_part = 4;

-- 100 distinct paths, only 2 fit in dynamic paths. Rest go to shared data.
INSERT INTO t_json_multi_bucket SELECT number, '{"path_' || toString(number) || '": ' || toString(number) || '}' FROM numbers(100);

SELECT '-- Wide part, non-empty shared data: multiple buckets expected';
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_json_multi_bucket' AND active = 1 LIMIT 1;

SELECT
    length(arrayFilter(x -> x LIKE 'j.object\_shared\_data.%.data', substreams)) AS shared_data_bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_json_multi_bucket' AND column = 'j' AND active = 1
LIMIT 1;

DROP TABLE t_json_multi_bucket;

-- ==========================================
-- Section 3: Compact part - buckets always used regardless of shared data emptiness
-- The single-bucket optimization only applies to Wide parts.
-- ==========================================

DROP TABLE IF EXISTS t_json_compact;

CREATE TABLE t_json_compact (id UInt64, j JSON(max_dynamic_paths = 100))
ENGINE = MergeTree ORDER BY id
SETTINGS
    min_bytes_for_wide_part = '200G',
    min_rows_for_wide_part = 1000000,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
    object_shared_data_buckets_for_compact_part = 4;

-- Only 2 paths, all fit in dynamic paths. But Compact part → still uses configured buckets.
INSERT INTO t_json_compact SELECT number, '{"a": ' || toString(number) || ', "b": ' || toString(number + 1) || '}' FROM numbers(100);

SELECT '-- Compact part, empty shared data: still uses configured buckets';
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_json_compact' AND active = 1 LIMIT 1;

SELECT
    length(arrayFilter(x -> x LIKE 'j.object\_shared\_data.%.data', substreams)) AS shared_data_bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_json_compact' AND column = 'j' AND active = 1
LIMIT 1;

DROP TABLE t_json_compact;

-- ==========================================
-- Section 4: Merge with empty shared data keeps single bucket
-- After merge, if shared data is still empty, the merged part should also have 1 bucket.
-- ==========================================

DROP TABLE IF EXISTS t_json_merge_single;

CREATE TABLE t_json_merge_single (id UInt64, j JSON(max_dynamic_paths = 100))
ENGINE = MergeTree ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
    object_shared_data_buckets_for_wide_part = 16;

INSERT INTO t_json_merge_single SELECT number, '{"a": ' || toString(number) || '}' FROM numbers(50);
INSERT INTO t_json_merge_single SELECT number + 50, '{"a": ' || toString(number + 50) || '}' FROM numbers(50);

OPTIMIZE TABLE t_json_merge_single FINAL;

SELECT '-- After merge, empty shared data: single bucket';
SELECT
    length(arrayFilter(x -> x LIKE 'j.object\_shared\_data.%.data', substreams)) AS shared_data_bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_json_merge_single' AND column = 'j' AND active = 1
LIMIT 1;

DROP TABLE t_json_merge_single;

-- ==========================================
-- Section 5: Correctness - data is readable after single-bucket optimization
-- ==========================================

DROP TABLE IF EXISTS t_json_correct;

CREATE TABLE t_json_correct (id UInt64, j JSON(max_dynamic_paths = 100))
ENGINE = MergeTree ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
    object_shared_data_buckets_for_wide_part = 32;

INSERT INTO t_json_correct SELECT number, '{"x": ' || toString(number) || ', "y": "val_' || toString(number) || '"}' FROM numbers(5);

SELECT '-- Correctness: read data from single-bucket part';
SELECT id, j.x, j.y FROM t_json_correct ORDER BY id;

OPTIMIZE TABLE t_json_correct FINAL;

SELECT '-- Correctness: read data after merge';
SELECT id, j.x, j.y FROM t_json_correct ORDER BY id;

DROP TABLE t_json_correct;

-- ==========================================
-- Section 6: Merge with non-empty shared data keeps multiple buckets
-- After merge, if shared data is non-empty, the merged part uses configured buckets.
-- ==========================================

DROP TABLE IF EXISTS t_json_merge_multi;

CREATE TABLE t_json_merge_multi (id UInt64, j JSON(max_dynamic_paths = 2))
ENGINE = MergeTree ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    object_serialization_version = 'v3',
    object_shared_data_serialization_version = 'advanced',
    object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
    object_shared_data_buckets_for_wide_part = 4;

INSERT INTO t_json_merge_multi SELECT number, '{"path_' || toString(number) || '": ' || toString(number) || '}' FROM numbers(50);
INSERT INTO t_json_merge_multi SELECT number + 50, '{"path_' || toString(number + 50) || '": ' || toString(number + 50) || '}' FROM numbers(50);

OPTIMIZE TABLE t_json_merge_multi FINAL;

SELECT '-- After merge, non-empty shared data: multiple buckets';
SELECT
    length(arrayFilter(x -> x LIKE 'j.object\_shared\_data.%.data', substreams)) AS shared_data_bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_json_merge_multi' AND column = 'j' AND active = 1
LIMIT 1;

-- Correctness check: data is readable from merged multi-bucket part.
SELECT count() FROM t_json_merge_multi;

DROP TABLE t_json_merge_multi;
