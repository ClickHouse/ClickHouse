-- Test: Map 'with_buckets' serialization - creation, writing, and bucket count

-- ==========================================
-- Section 1: map_serialization_version_for_zero_level_parts
-- ==========================================

-- Case 1a: zero_level_parts='basic', merged='with_buckets'
-- INSERT creates basic (non-bucketed) zero-level parts.
-- OPTIMIZE FINAL converts them to with_buckets.
DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'basic',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_coefficient = 1.0,
    map_buckets_min_avg_size = 0,
    index_granularity = 8192,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    write_marks_for_substreams_in_compact_parts = 1,
    serialization_info_version = 'with_types';

INSERT INTO t SELECT number, map('key', number) FROM numbers(100);

-- All zero-level parts use basic: no 'm.buckets_info' stream
SELECT '1a: zero-level uses_with_buckets';
SELECT min(has(substreams, 'm.buckets_info')) AS uses_with_buckets
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0;

OPTIMIZE TABLE t FINAL;

-- After merge: part uses with_buckets
SELECT '1a: merged uses_with_buckets';
SELECT has(substreams, 'm.buckets_info') AS uses_with_buckets
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

DROP TABLE t;

-- Case 1b: zero_level_parts='with_buckets', merged='with_buckets'
-- INSERT already creates with_buckets zero-level parts.
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_coefficient = 1.0,
    map_buckets_min_avg_size = 0,
    index_granularity = 8192,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    write_marks_for_substreams_in_compact_parts = 1,
    serialization_info_version = 'with_types';

INSERT INTO t SELECT number, map('key', number) FROM numbers(100);

-- Zero-level parts already use with_buckets
SELECT '1b: zero-level uses_with_buckets';
SELECT min(has(substreams, 'm.buckets_info')) AS uses_with_buckets
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0;

OPTIMIZE TABLE t FINAL;

-- After merge: still with_buckets
SELECT '1b: merged uses_with_buckets';
SELECT has(substreams, 'm.buckets_info') AS uses_with_buckets
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

DROP TABLE t;


-- ==========================================
-- Section 2: Wide and Compact parts both support with_buckets
-- ==========================================

-- Case 2a: Wide parts
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_coefficient = 1.0,
    map_buckets_min_avg_size = 0,
    index_granularity = 8192,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    write_marks_for_substreams_in_compact_parts = 1,
    serialization_info_version = 'with_types';

INSERT INTO t SELECT number, map('k1', number, 'k2', number + 1) FROM numbers(100);
OPTIMIZE TABLE t FINAL;

SELECT '2a: part_type';
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active = 1 LIMIT 1;

SELECT '2a: uses_with_buckets, bucket_count';
SELECT has(substreams, 'm.buckets_info') AS uses_with_buckets,
       length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

DROP TABLE t;

-- Case 2b: Compact parts
CREATE TABLE t (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_coefficient = 1.0,
    map_buckets_min_avg_size = 0,
    index_granularity = 8192,
    min_bytes_for_wide_part = '200G',
    min_rows_for_wide_part = 1000000,
    write_marks_for_substreams_in_compact_parts = 1,
    serialization_info_version = 'with_types';

INSERT INTO t SELECT number, map('k1', number, 'k2', number + 1) FROM numbers(100);
OPTIMIZE TABLE t FINAL;

SELECT '2b: part_type';
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active = 1 LIMIT 1;

SELECT '2b: uses_with_buckets, bucket_count';
SELECT has(substreams, 'm.buckets_info') AS uses_with_buckets,
       length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

DROP TABLE t;


-- ==========================================
-- Section 3: Bucket count - strategy = 'constant'
-- Formula: always max_buckets_in_map, regardless of map size
-- ==========================================

CREATE TABLE t (id UInt64, m Map(UInt64, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_coefficient = 1.0,
    map_buckets_min_avg_size = 0,
    index_granularity = 8192,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    write_marks_for_substreams_in_compact_parts = 1,
    serialization_info_version = 'with_types';

-- avg = 1 key per map: constant strategy ignores size → always 4 buckets
INSERT INTO t SELECT number, mapFromArrays(range(1), range(1)) FROM numbers(100);

-- Zero-level parts: constant strategy applies immediately at write time
SELECT '3: constant avg=1 max=4, zero-level bucket_count';
SELECT max(length(arrayFilter(x -> x LIKE '%.size0', substreams))) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0;

OPTIMIZE TABLE t FINAL;

SELECT '3: constant avg=1 max=4, merged bucket_count';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

DROP TABLE t;


-- ==========================================
-- Section 4: Bucket count - strategy = 'sqrt'
-- Formula: max(1, min(max_buckets_in_map, round(map_buckets_coefficient * sqrt(avg_size))))
-- ==========================================

CREATE TABLE t (id UInt64, m Map(UInt64, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 8,
    map_buckets_strategy = 'sqrt',
    map_buckets_coefficient = 1.0,
    map_buckets_min_avg_size = 0,
    index_granularity = 8192,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    write_marks_for_substreams_in_compact_parts = 1,
    serialization_info_version = 'with_types';

-- avg = 4: round(1.0 * sqrt(4)) = round(2.0) = 2 buckets
INSERT INTO t SELECT number, mapFromArrays(range(4), range(4)) FROM numbers(100);

SELECT '4: sqrt coeff=1 avg=4 max=8, zero-level bucket_count';
SELECT max(length(arrayFilter(x -> x LIKE '%.size0', substreams))) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0;

OPTIMIZE TABLE t FINAL;

SELECT '4: sqrt coeff=1 avg=4 max=8, merged bucket_count';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

TRUNCATE TABLE t;

-- avg = 9: round(1.0 * sqrt(9)) = round(3.0) = 3 buckets
INSERT INTO t SELECT number, mapFromArrays(range(9), range(9)) FROM numbers(100);

SELECT '4: sqrt coeff=1 avg=9 max=8, zero-level bucket_count';
SELECT max(length(arrayFilter(x -> x LIKE '%.size0', substreams))) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0;

OPTIMIZE TABLE t FINAL;

SELECT '4: sqrt coeff=1 avg=9 max=8, merged bucket_count';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

DROP TABLE t;

-- Coefficient != 1.0: avg=4, coeff=2.0, max=8 → round(2.0 * sqrt(4)) = round(4.0) = 4 buckets
CREATE TABLE t (id UInt64, m Map(UInt64, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 8,
    map_buckets_strategy = 'sqrt',
    map_buckets_coefficient = 2.0,
    map_buckets_min_avg_size = 0,
    index_granularity = 8192,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    write_marks_for_substreams_in_compact_parts = 1,
    serialization_info_version = 'with_types';

INSERT INTO t SELECT number, mapFromArrays(range(4), range(4)) FROM numbers(100);

SELECT '4: sqrt coeff=2 avg=4 max=8, zero-level bucket_count';
SELECT max(length(arrayFilter(x -> x LIKE '%.size0', substreams))) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0;

OPTIMIZE TABLE t FINAL;

SELECT '4: sqrt coeff=2 avg=4 max=8, merged bucket_count';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

DROP TABLE t;

-- max_buckets_in_map cap: avg=16, coeff=1.0, max=3 → round(sqrt(16)) = 4, capped to 3
CREATE TABLE t (id UInt64, m Map(UInt64, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 3,
    map_buckets_strategy = 'sqrt',
    map_buckets_coefficient = 1.0,
    map_buckets_min_avg_size = 0,
    index_granularity = 8192,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    write_marks_for_substreams_in_compact_parts = 1,
    serialization_info_version = 'with_types';

INSERT INTO t SELECT number, mapFromArrays(range(16), range(16)) FROM numbers(100);

SELECT '4: sqrt coeff=1 avg=16 max=3 (capped), zero-level bucket_count';
SELECT max(length(arrayFilter(x -> x LIKE '%.size0', substreams))) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0;

OPTIMIZE TABLE t FINAL;

SELECT '4: sqrt coeff=1 avg=16 max=3 (capped), merged bucket_count';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

DROP TABLE t;


-- ==========================================
-- Section 5: Bucket count - strategy = 'linear'
-- Formula: max(1, min(max_buckets_in_map, round(map_buckets_coefficient * avg_size)))
-- ==========================================

CREATE TABLE t (id UInt64, m Map(UInt64, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 8,
    map_buckets_strategy = 'linear',
    map_buckets_coefficient = 1.0,
    map_buckets_min_avg_size = 0,
    index_granularity = 8192,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    write_marks_for_substreams_in_compact_parts = 1,
    serialization_info_version = 'with_types';

-- avg = 2: round(1.0 * 2) = 2 buckets
INSERT INTO t SELECT number, mapFromArrays(range(2), range(2)) FROM numbers(100);

SELECT '5: linear coeff=1 avg=2 max=8, zero-level bucket_count';
SELECT max(length(arrayFilter(x -> x LIKE '%.size0', substreams))) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0;

OPTIMIZE TABLE t FINAL;

SELECT '5: linear coeff=1 avg=2 max=8, merged bucket_count';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

TRUNCATE TABLE t;

-- avg = 3: round(1.0 * 3) = 3 buckets
INSERT INTO t SELECT number, mapFromArrays(range(3), range(3)) FROM numbers(100);

SELECT '5: linear coeff=1 avg=3 max=8, zero-level bucket_count';
SELECT max(length(arrayFilter(x -> x LIKE '%.size0', substreams))) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0;

OPTIMIZE TABLE t FINAL;

SELECT '5: linear coeff=1 avg=3 max=8, merged bucket_count';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

DROP TABLE t;

-- Coefficient != 1.0: avg=3, coeff=2.0, max=8 → round(2.0 * 3) = round(6.0) = 6 buckets
CREATE TABLE t (id UInt64, m Map(UInt64, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 8,
    map_buckets_strategy = 'linear',
    map_buckets_coefficient = 2.0,
    map_buckets_min_avg_size = 0,
    index_granularity = 8192,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    write_marks_for_substreams_in_compact_parts = 1,
    serialization_info_version = 'with_types';

INSERT INTO t SELECT number, mapFromArrays(range(3), range(3)) FROM numbers(100);

SELECT '5: linear coeff=2 avg=3 max=8, zero-level bucket_count';
SELECT max(length(arrayFilter(x -> x LIKE '%.size0', substreams))) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0;

OPTIMIZE TABLE t FINAL;

SELECT '5: linear coeff=2 avg=3 max=8, merged bucket_count';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

DROP TABLE t;

-- max_buckets_in_map cap: avg=10, coeff=1.0, max=4 → round(10) = 10, capped to 4
CREATE TABLE t (id UInt64, m Map(UInt64, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'linear',
    map_buckets_coefficient = 1.0,
    map_buckets_min_avg_size = 0,
    index_granularity = 8192,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    write_marks_for_substreams_in_compact_parts = 1,
    serialization_info_version = 'with_types';

INSERT INTO t SELECT number, mapFromArrays(range(10), range(10)) FROM numbers(100);

SELECT '5: linear coeff=1 avg=10 max=4 (capped), zero-level bucket_count';
SELECT max(length(arrayFilter(x -> x LIKE '%.size0', substreams))) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0;

OPTIMIZE TABLE t FINAL;

SELECT '5: linear coeff=1 avg=10 max=4 (capped), merged bucket_count';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

DROP TABLE t;


-- ==========================================
-- Section 6: map_buckets_min_avg_size threshold
-- If avg_size < min_avg_size: always 1 bucket (no bucketing overhead for small maps)
-- If avg_size >= min_avg_size: formula applies normally
-- ==========================================

CREATE TABLE t (id UInt64, m Map(UInt64, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 8,
    map_buckets_strategy = 'sqrt',
    map_buckets_coefficient = 1.0,
    map_buckets_min_avg_size = 4,
    index_granularity = 8192,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    write_marks_for_substreams_in_compact_parts = 1,
    serialization_info_version = 'with_types';

-- avg = 1 < min_avg_size = 4: use 1 bucket regardless of formula
INSERT INTO t SELECT number, mapFromArrays(range(1), range(1)) FROM numbers(100);

SELECT '6: sqrt min_avg=4 avg=1 (below threshold), zero-level bucket_count';
SELECT max(length(arrayFilter(x -> x LIKE '%.size0', substreams))) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0;

OPTIMIZE TABLE t FINAL;

SELECT '6: sqrt min_avg=4 avg=1 (below threshold), merged bucket_count';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

TRUNCATE TABLE t;

-- avg = 3 < min_avg_size = 4: still 1 bucket
INSERT INTO t SELECT number, mapFromArrays(range(3), range(3)) FROM numbers(100);

SELECT '6: sqrt min_avg=4 avg=3 (below threshold), zero-level bucket_count';
SELECT max(length(arrayFilter(x -> x LIKE '%.size0', substreams))) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0;

OPTIMIZE TABLE t FINAL;

SELECT '6: sqrt min_avg=4 avg=3 (below threshold), merged bucket_count';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

TRUNCATE TABLE t;

-- avg = 4 = min_avg_size = 4: threshold NOT triggered (avg < min_avg_size is false)
-- Formula applies: round(sqrt(4)) = 2 buckets
INSERT INTO t SELECT number, mapFromArrays(range(4), range(4)) FROM numbers(100);

SELECT '6: sqrt min_avg=4 avg=4 (at threshold), zero-level bucket_count';
SELECT max(length(arrayFilter(x -> x LIKE '%.size0', substreams))) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0;

OPTIMIZE TABLE t FINAL;

SELECT '6: sqrt min_avg=4 avg=4 (at threshold), merged bucket_count';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

TRUNCATE TABLE t;

-- avg = 9 > min_avg_size = 4: formula applies: round(sqrt(9)) = 3 buckets
INSERT INTO t SELECT number, mapFromArrays(range(9), range(9)) FROM numbers(100);

SELECT '6: sqrt min_avg=4 avg=9 (above threshold), zero-level bucket_count';
SELECT max(length(arrayFilter(x -> x LIKE '%.size0', substreams))) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1 AND level = 0;

OPTIMIZE TABLE t FINAL;

SELECT '6: sqrt min_avg=4 avg=9 (above threshold), merged bucket_count';
SELECT length(arrayFilter(x -> x LIKE '%.size0', substreams)) AS bucket_count
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't' AND column = 'm' AND active = 1
LIMIT 1;

DROP TABLE t;
