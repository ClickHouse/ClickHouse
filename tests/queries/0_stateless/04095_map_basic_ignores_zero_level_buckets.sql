-- Regression test: map_serialization_version = 'basic' must prevent bucketed
-- serialization in zero-level parts, even when map_serialization_version_for_zero_level_parts
-- is set to 'with_buckets'. Without the fix in MergeTreeDataWriter, the keys
-- would be reordered by hash bucket, breaking insertion order.

DROP TABLE IF EXISTS t_map_basic_zero_level;

-- Simulate the conflicting combination: basic base + bucketed zero-level.
-- The fix caps zero-level serialization at the base level, so 'basic' wins.
CREATE TABLE t_map_basic_zero_level (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'basic',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_coefficient = 1.0,
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = '200G',
    min_rows_for_wide_part = 1000000;

INSERT INTO t_map_basic_zero_level VALUES
    (1, {'a' : 1, 'b' : 2, 'c' : 3}),
    (2, {'c' : 100, 'd' : 200}),
    (3, {'a' : 1, 'b' : 2, 'c' : 3, 'd' : 4, 'e' : 5});

-- With the bug, keys would be reordered (e.g. {'d':200,'c':100} instead of {'c':100,'d':200})
SELECT m FROM t_map_basic_zero_level ORDER BY id;
SELECT m.keys FROM t_map_basic_zero_level ORDER BY id;
SELECT m.values FROM t_map_basic_zero_level ORDER BY id;

DROP TABLE t_map_basic_zero_level;
