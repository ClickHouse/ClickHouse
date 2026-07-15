-- Test: Changing map_serialization_version and bucket settings on existing table.
-- Exercises merging parts with different serialization modes and bucket counts.

-- ==========================================
-- Section 1: Change from basic to with_buckets
-- Insert data as basic, change setting, insert more, merge, verify data.
-- ==========================================

DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'basic',
    map_serialization_version_for_zero_level_parts = 'basic',
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

INSERT INTO t SELECT number, map('a', number, 'b', number + 1) FROM numbers(50);

SELECT '1: basic parts, no buckets_info';
SELECT min(has(substreams, 'm.buckets_info')) AS uses_with_buckets
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1;

-- Change serialization version to with_buckets
ALTER TABLE t MODIFY SETTING
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0;

-- Insert new data — these parts should use with_buckets
INSERT INTO t SELECT number + 50, map('a', number + 50, 'c', number + 100) FROM numbers(50);

SELECT '1: after alter, new parts use with_buckets';
SELECT has(substreams, 'm.buckets_info') AS uses_with_buckets
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0
ORDER BY name DESC
LIMIT 1;

-- Merge all: basic + with_buckets parts merged together
OPTIMIZE TABLE t FINAL;

SELECT '1: merged part uses with_buckets';
SELECT has(substreams, 'm.buckets_info') AS uses_with_buckets
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

-- Verify data correctness after merge
SELECT '1: row count after merge';
SELECT count() FROM t;
SELECT '1: spot check data';
SELECT id, m FROM t WHERE id IN (0, 25, 49, 50, 75, 99) ORDER BY id;
SELECT '1: subcolumn correctness';
SELECT id, m.key_a, m.key_b, m.key_c FROM t WHERE id IN (0, 25, 49, 50, 75, 99) ORDER BY id;

DROP TABLE t;

-- ==========================================
-- Section 2: Change from with_buckets to basic
-- Insert data as with_buckets, change to basic, insert more, merge.
-- ==========================================

DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

INSERT INTO t SELECT number, map('x', number, 'y', number * 10) FROM numbers(50);

SELECT '2: initial with_buckets parts';
SELECT min(has(substreams, 'm.buckets_info')) AS uses_with_buckets
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1;

-- Change to basic
ALTER TABLE t MODIFY SETTING
    map_serialization_version = 'basic',
    map_serialization_version_for_zero_level_parts = 'basic';

-- Insert new data as basic
INSERT INTO t SELECT number + 50, map('x', number + 50, 'z', number * 5) FROM numbers(50);

SELECT '2: new parts use basic';
SELECT has(substreams, 'm.buckets_info') AS uses_with_buckets
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0
ORDER BY name DESC
LIMIT 1;

-- Merge: with_buckets + basic parts merged together
OPTIMIZE TABLE t FINAL;

SELECT '2: merged part uses basic';
SELECT has(substreams, 'm.buckets_info') AS uses_with_buckets
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

SELECT '2: row count after merge';
SELECT count() FROM t;
SELECT '2: spot check data';
SELECT id, m FROM t WHERE id IN (0, 25, 49, 50, 75, 99) ORDER BY id;
SELECT '2: subcolumn correctness';
SELECT id, m.key_x, m.key_y, m.key_z FROM t WHERE id IN (0, 25, 49, 50, 75, 99) ORDER BY id;

DROP TABLE t;

-- ==========================================
-- Section 3: Change bucket count (different max_buckets_in_map)
-- Insert with 4 buckets, change to 8 buckets, insert, merge.
-- ==========================================

DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

INSERT INTO t SELECT number, map('a', number, 'b', number + 1, 'c', number + 2) FROM numbers(50);

SELECT '3: initial 4-bucket parts';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0
LIMIT 1;

-- Change to 8 buckets
ALTER TABLE t MODIFY SETTING max_buckets_in_map = 8;

INSERT INTO t SELECT number + 50, map('a', number + 50, 'b', number + 51, 'd', number + 52) FROM numbers(50);

SELECT '3: new parts use 8 buckets';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0
ORDER BY name DESC
LIMIT 1;

-- Merge 4-bucket + 8-bucket parts
OPTIMIZE TABLE t FINAL;

SELECT '3: merged part bucket count';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

SELECT '3: row count after merge';
SELECT count() FROM t;
SELECT '3: spot check data';
SELECT id, m FROM t WHERE id IN (0, 25, 49, 50, 75, 99) ORDER BY id;
SELECT '3: subcolumn correctness';
SELECT id, m.key_a, m.key_b, m.key_c, m.key_d FROM t WHERE id IN (0, 25, 49, 50, 75, 99) ORDER BY id;

DROP TABLE t;

-- ==========================================
-- Section 4: Change bucket strategy
-- Insert with constant strategy, change to sqrt, insert, merge.
-- ==========================================

DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(UInt64, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 8,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

-- avg = 4 keys, constant strategy: always 8 buckets
INSERT INTO t SELECT number, mapFromArrays(range(4), range(4)) FROM numbers(50);

SELECT '4: constant strategy, 8 buckets';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0
LIMIT 1;

-- Change to sqrt strategy: round(sqrt(4)) = 2 buckets
ALTER TABLE t MODIFY SETTING map_buckets_strategy = 'sqrt';

INSERT INTO t SELECT number + 50, mapFromArrays(range(4), range(4)) FROM numbers(50);

SELECT '4: sqrt strategy, 2 buckets for new part';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0
ORDER BY name DESC
LIMIT 1;

-- Merge 8-bucket + 2-bucket parts; merged should use sqrt → 2 buckets
OPTIMIZE TABLE t FINAL;

SELECT '4: merged with sqrt strategy';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

SELECT '4: row count after merge';
SELECT count() FROM t;
SELECT '4: correctness check';
SELECT sum(m[0]) AS s0, sum(m[1]) AS s1, sum(m[2]) AS s2, sum(m[3]) AS s3 FROM t;

DROP TABLE t;

-- ==========================================
-- Section 5: Multiple inserts with different settings, then single merge
-- Exercises merging 3+ parts with heterogeneous formats.
-- ==========================================

DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'basic',
    map_serialization_version_for_zero_level_parts = 'basic',
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

-- Part 1: basic serialization
INSERT INTO t SELECT number, map('a', number) FROM numbers(30);

-- Change to with_buckets, 2 buckets constant
ALTER TABLE t MODIFY SETTING
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 2,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0;

-- Part 2: with_buckets, 2 buckets
INSERT INTO t SELECT number + 30, map('a', number + 30, 'b', number + 100) FROM numbers(30);

-- Change to 6 buckets constant
ALTER TABLE t MODIFY SETTING max_buckets_in_map = 6;

-- Part 3: with_buckets, 6 buckets
INSERT INTO t SELECT number + 60, map('a', number + 60, 'b', number + 200, 'c', number + 300) FROM numbers(30);

SELECT '5: three parts with different formats';
SELECT name,
       has(substreams, 'm.buckets_info') AS uses_with_buckets,
       length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0
ORDER BY name;

-- Merge all three heterogeneous parts
OPTIMIZE TABLE t FINAL;

SELECT '5: merged part format';
SELECT has(substreams, 'm.buckets_info') AS uses_with_buckets,
       length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

SELECT '5: row count after merge';
SELECT count() FROM t;
SELECT '5: spot check data';
SELECT id, m FROM t WHERE id IN (0, 15, 29, 30, 45, 59, 60, 75, 89) ORDER BY id;
SELECT '5: subcolumn correctness';
SELECT id, m.key_a, m.key_b, m.key_c FROM t WHERE id IN (0, 15, 29, 30, 45, 59, 60, 75, 89) ORDER BY id;

DROP TABLE t;

-- ==========================================
-- Section 6: Change zero-level serialization independently
-- Merged parts use with_buckets, zero-level uses basic, then swap.
-- ==========================================

DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'basic',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types';

INSERT INTO t SELECT number, map('a', number, 'b', number * 2) FROM numbers(50);

SELECT '6: zero-level parts are basic';
SELECT min(has(substreams, 'm.buckets_info')) AS uses_with_buckets
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0;

-- Now change zero-level to with_buckets too
ALTER TABLE t MODIFY SETTING map_serialization_version_for_zero_level_parts = 'with_buckets';

INSERT INTO t SELECT number + 50, map('a', number + 50, 'c', number * 3) FROM numbers(50);

SELECT '6: new zero-level parts are with_buckets';
SELECT has(substreams, 'm.buckets_info') AS uses_with_buckets
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0
ORDER BY name DESC
LIMIT 1;

-- Merge basic zero-level + with_buckets zero-level → merged with_buckets
OPTIMIZE TABLE t FINAL;

SELECT '6: merged uses with_buckets';
SELECT has(substreams, 'm.buckets_info') AS uses_with_buckets
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

SELECT '6: row count after merge';
SELECT count() FROM t;
SELECT '6: subcolumn correctness';
SELECT id, m.key_a, m.key_b, m.key_c FROM t WHERE id IN (0, 25, 49, 50, 75, 99) ORDER BY id;

DROP TABLE t;
