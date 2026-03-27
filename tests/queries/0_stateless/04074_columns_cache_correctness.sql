-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- Comprehensive correctness tests for columns cache feature

SET max_threads = 1;
SET use_columns_cache = 1;
SYSTEM DROP COLUMNS CACHE;

-- ============================================================================
-- Test 1: Different column types
-- ============================================================================

DROP TABLE IF EXISTS t_cache_types;

CREATE TABLE t_cache_types (
    id UInt64,
    int8_col Int8,
    int16_col Int16,
    int32_col Int32,
    int64_col Int64,
    uint8_col UInt8,
    uint16_col UInt16,
    uint32_col UInt32,
    uint64_col UInt64,
    float32_col Float32,
    float64_col Float64,
    string_col String,
    fixed_string_col FixedString(10),
    date_col Date,
    datetime_col DateTime,
    nullable_col Nullable(UInt64),
    array_col Array(UInt64),
    tuple_col Tuple(UInt64, String),
    enum_col Enum8('a' = 1, 'b' = 2, 'c' = 3),
    decimal_col Decimal(18, 4)
) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_types SELECT
    number,
    number % 128 - 64,
    number % 32768 - 16384,
    number,
    number,
    number % 256,
    number % 65536,
    number,
    number,
    number * 1.5,
    number * 2.5,
    toString(number),
    leftPad(toString(number % 1000), 10, '0'),
    toDate('2024-01-01') + number % 365,
    toDateTime('2024-01-01 00:00:00') + number,
    if(number % 10 = 0, NULL, number),
    [number, number * 2, number * 3],
    tuple(number, toString(number)),
    if(number % 3 = 0, 'a', if(number % 3 = 1, 'b', 'c')),
    number + number / 100.0
FROM numbers(10000);

-- Read all column types without cache
SELECT
    sum(int8_col), sum(int16_col), sum(int32_col), sum(int64_col),
    sum(uint8_col), sum(uint16_col), sum(uint32_col), sum(uint64_col),
    sum(float32_col), sum(float64_col),
    count(string_col), count(fixed_string_col),
    count(date_col), count(datetime_col),
    count(nullable_col), sum(nullable_col),
    sum(length(array_col)), count()
FROM t_cache_types
SETTINGS use_columns_cache = 0;

-- Read with cache (populate)
SELECT
    sum(int8_col), sum(int16_col), sum(int32_col), sum(int64_col),
    sum(uint8_col), sum(uint16_col), sum(uint32_col), sum(uint64_col),
    sum(float32_col), sum(float64_col),
    count(string_col), count(fixed_string_col),
    count(date_col), count(datetime_col),
    count(nullable_col), sum(nullable_col),
    sum(length(array_col)), count()
FROM t_cache_types
SETTINGS use_columns_cache = 1;

-- Read with cache (should hit)
SELECT
    sum(int8_col), sum(int16_col), sum(int32_col), sum(int64_col),
    sum(uint8_col), sum(uint16_col), sum(uint32_col), sum(uint64_col),
    sum(float32_col), sum(float64_col),
    count(string_col), count(fixed_string_col),
    count(date_col), count(datetime_col),
    count(nullable_col), sum(nullable_col),
    sum(length(array_col)), count()
FROM t_cache_types
SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_types;

-- ============================================================================
-- Test 2: Cache invalidation on table operations
-- ============================================================================

DROP TABLE IF EXISTS t_cache_invalidation;
DROP TABLE IF EXISTS t_cache_renamed;

CREATE TABLE t_cache_invalidation (id UInt64, value String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_invalidation SELECT number, toString(number) FROM numbers(5000);

SYSTEM DROP COLUMNS CACHE;

-- Populate cache
SELECT sum(id), count() FROM t_cache_invalidation SETTINGS use_columns_cache = 1;

-- Rename table (should invalidate cache due to UUID change)
RENAME TABLE t_cache_invalidation TO t_cache_renamed;

-- Query renamed table (cache should miss, but result should be correct)
SELECT sum(id), count() FROM t_cache_renamed SETTINGS use_columns_cache = 1;

-- Query again (should populate cache with new UUID)
SELECT sum(id), count() FROM t_cache_renamed SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_renamed;

-- ============================================================================
-- Test 3: Multiple parts and merges
-- ============================================================================

DROP TABLE IF EXISTS t_cache_multipart;

CREATE TABLE t_cache_multipart (id UInt64, value UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

-- Insert multiple parts
INSERT INTO t_cache_multipart SELECT number, number * 2 FROM numbers(2000);
INSERT INTO t_cache_multipart SELECT number + 2000, (number + 2000) * 2 FROM numbers(2000);
INSERT INTO t_cache_multipart SELECT number + 4000, (number + 4000) * 2 FROM numbers(2000);
INSERT INTO t_cache_multipart SELECT number + 6000, (number + 6000) * 2 FROM numbers(2000);

SYSTEM DROP COLUMNS CACHE;

-- Read all parts (populate cache)
SELECT sum(id), sum(value), count() FROM t_cache_multipart SETTINGS use_columns_cache = 1;

-- Read again (should hit cache)
SELECT sum(id), sum(value), count() FROM t_cache_multipart SETTINGS use_columns_cache = 1;

-- Optimize table (merge parts)
OPTIMIZE TABLE t_cache_multipart FINAL;

-- Read after merge (cache should miss for new merged part)
SELECT sum(id), sum(value), count() FROM t_cache_multipart SETTINGS use_columns_cache = 1;

-- Read again after merge (should hit cache for new merged part)
SELECT sum(id), sum(value), count() FROM t_cache_multipart SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_multipart;

-- ============================================================================
-- Test 4: Partial mark range reads
-- ============================================================================

DROP TABLE IF EXISTS t_cache_partial;

CREATE TABLE t_cache_partial (id UInt64, value UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_partial SELECT number, number * 3 FROM numbers(10000);

SYSTEM DROP COLUMNS CACHE;

-- Read subset 1
SELECT sum(id), count() FROM t_cache_partial WHERE id < 3000 SETTINGS use_columns_cache = 1;

-- Read subset 2 (overlapping)
SELECT sum(id), count() FROM t_cache_partial WHERE id < 5000 SETTINGS use_columns_cache = 1;

-- Read subset 3 (non-overlapping)
SELECT sum(id), count() FROM t_cache_partial WHERE id >= 7000 SETTINGS use_columns_cache = 1;

-- Read all (should use cached parts and cache new parts)
SELECT sum(id), count() FROM t_cache_partial SETTINGS use_columns_cache = 1;

-- Read subsets again (should hit cache)
SELECT sum(id), count() FROM t_cache_partial WHERE id < 3000 SETTINGS use_columns_cache = 1;
SELECT sum(id), count() FROM t_cache_partial WHERE id >= 7000 SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_partial;

-- ============================================================================
-- Test 5: PREWHERE optimization with cache
-- ============================================================================

DROP TABLE IF EXISTS t_cache_prewhere;

CREATE TABLE t_cache_prewhere (
    id UInt64,
    filter_col UInt64,
    heavy_col String
) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_prewhere SELECT
    number,
    number % 100,
    repeat(toString(number), 100)
FROM numbers(5000);

SYSTEM DROP COLUMNS CACHE;

-- Query with PREWHERE
SELECT sum(id), count() FROM t_cache_prewhere
PREWHERE filter_col < 10
SETTINGS use_columns_cache = 1;

-- Repeat (should hit cache for PREWHERE column)
SELECT sum(id), count() FROM t_cache_prewhere
PREWHERE filter_col < 10
SETTINGS use_columns_cache = 1;

-- Query heavy column with PREWHERE
SELECT sum(id), sum(length(heavy_col)), count() FROM t_cache_prewhere
PREWHERE filter_col < 10
WHERE id < 2000
SETTINGS use_columns_cache = 1;

-- Repeat (should hit cache)
SELECT sum(id), sum(length(heavy_col)), count() FROM t_cache_prewhere
PREWHERE filter_col < 10
WHERE id < 2000
SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_prewhere;

-- ============================================================================
-- Test 6: Settings combinations
-- ============================================================================

DROP TABLE IF EXISTS t_cache_settings;

CREATE TABLE t_cache_settings (id UInt64, value UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_settings SELECT number, number * 4 FROM numbers(3000);

SYSTEM DROP COLUMNS CACHE;

-- Test: enable_writes_to_columns_cache = 0
SELECT sum(id), count() FROM t_cache_settings
SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 0;

-- Cache should be empty, so this will populate it
SELECT sum(id), count() FROM t_cache_settings
SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 1;

-- This should hit cache
SELECT sum(id), count() FROM t_cache_settings
SETTINGS use_columns_cache = 1;

SYSTEM DROP COLUMNS CACHE;

-- Populate cache
SELECT sum(id), count() FROM t_cache_settings
SETTINGS use_columns_cache = 1;

-- Test: enable_reads_from_columns_cache = 0 (should not use cache but produce correct result)
SELECT sum(id), count() FROM t_cache_settings
SETTINGS use_columns_cache = 1, enable_reads_from_columns_cache = 0;

DROP TABLE t_cache_settings;

-- ============================================================================
-- Test 7: Empty results
-- ============================================================================

DROP TABLE IF EXISTS t_cache_empty;

CREATE TABLE t_cache_empty (id UInt64, value String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_empty SELECT number, toString(number) FROM numbers(5000);

SYSTEM DROP COLUMNS CACHE;

-- Query with no results
SELECT sum(id), count() FROM t_cache_empty WHERE id > 10000 SETTINGS use_columns_cache = 1;

-- Repeat
SELECT sum(id), count() FROM t_cache_empty WHERE id > 10000 SETTINGS use_columns_cache = 1;

-- Query with results
SELECT sum(id), count() FROM t_cache_empty WHERE id < 100 SETTINGS use_columns_cache = 1;

-- Repeat
SELECT sum(id), count() FROM t_cache_empty WHERE id < 100 SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_empty;

-- ============================================================================
-- Test 8: Nested columns and arrays
-- ============================================================================

DROP TABLE IF EXISTS t_cache_nested;

CREATE TABLE t_cache_nested (
    id UInt64,
    arr Array(Array(UInt64)),
    map_col Map(String, UInt64),
    nested Nested(a UInt64, b String)
) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_nested SELECT
    number,
    [[number, number + 1], [number + 2, number + 3]],
    map('key' || toString(number), number),
    [number, number + 1],
    [toString(number), toString(number + 1)]
FROM numbers(3000);

SYSTEM DROP COLUMNS CACHE;

-- Read nested structures
SELECT sum(id), sum(length(arr)), sum(map_col['key100']), count() FROM t_cache_nested
SETTINGS use_columns_cache = 1;

-- Repeat
SELECT sum(id), sum(length(arr)), sum(map_col['key100']), count() FROM t_cache_nested
SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_nested;

-- ============================================================================
-- Test 9: Partitioned table
-- ============================================================================

DROP TABLE IF EXISTS t_cache_partitioned;

CREATE TABLE t_cache_partitioned (
    date Date,
    id UInt64,
    value String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_partitioned SELECT
    toDate('2024-01-01') + (number % 90),
    number,
    toString(number)
FROM numbers(10000);

SYSTEM DROP COLUMNS CACHE;

-- Read specific partition
SELECT sum(id), count() FROM t_cache_partitioned
WHERE date >= '2024-01-01' AND date < '2024-02-01'
SETTINGS use_columns_cache = 1;

-- Repeat
SELECT sum(id), count() FROM t_cache_partitioned
WHERE date >= '2024-01-01' AND date < '2024-02-01'
SETTINGS use_columns_cache = 1;

-- Read different partition
SELECT sum(id), count() FROM t_cache_partitioned
WHERE date >= '2024-02-01' AND date < '2024-03-01'
SETTINGS use_columns_cache = 1;

-- Read all partitions
SELECT sum(id), count() FROM t_cache_partitioned
SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_partitioned;

-- ============================================================================
-- Test 10: Large strings and compression
-- ============================================================================

DROP TABLE IF EXISTS t_cache_compression;

CREATE TABLE t_cache_compression (
    id UInt64,
    small_string String,
    large_string String CODEC(ZSTD),
    compressed_int UInt64 CODEC(Delta, LZ4)
) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_compression SELECT
    number,
    toString(number),
    repeat('data' || toString(number), 1000),
    number
FROM numbers(2000);

SYSTEM DROP COLUMNS CACHE;

-- Read compressed columns
SELECT sum(id), sum(length(large_string)), sum(compressed_int), count() FROM t_cache_compression
SETTINGS use_columns_cache = 1;

-- Repeat (should hit cache for decompressed data)
SELECT sum(id), sum(length(large_string)), sum(compressed_int), count() FROM t_cache_compression
SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_compression;

-- ============================================================================
-- Test 11: JOIN operations with cached tables
-- ============================================================================

DROP TABLE IF EXISTS t_cache_join1;
DROP TABLE IF EXISTS t_cache_join2;

CREATE TABLE t_cache_join1 (id UInt64, value1 String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

CREATE TABLE t_cache_join2 (id UInt64, value2 String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_join1 SELECT number, 'v1_' || toString(number) FROM numbers(3000);
INSERT INTO t_cache_join2 SELECT number, 'v2_' || toString(number) FROM numbers(3000);

SYSTEM DROP COLUMNS CACHE;

-- Populate cache for both tables
SELECT sum(id), count() FROM t_cache_join1 SETTINGS use_columns_cache = 1;
SELECT sum(id), count() FROM t_cache_join2 SETTINGS use_columns_cache = 1;

-- JOIN with cache
SELECT sum(t1.id), count()
FROM t_cache_join1 t1
INNER JOIN t_cache_join2 t2 ON t1.id = t2.id
WHERE t1.id < 1000
SETTINGS use_columns_cache = 1;

-- Repeat JOIN
SELECT sum(t1.id), count()
FROM t_cache_join1 t1
INNER JOIN t_cache_join2 t2 ON t1.id = t2.id
WHERE t1.id < 1000
SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_join1;
DROP TABLE t_cache_join2;

-- ============================================================================
-- Test 12: Subqueries with cache
-- ============================================================================

DROP TABLE IF EXISTS t_cache_subquery;

CREATE TABLE t_cache_subquery (id UInt64, category UInt8, value UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_subquery SELECT number, number % 10, number * 2 FROM numbers(5000);

SYSTEM DROP COLUMNS CACHE;

-- Subquery with cache
SELECT category, total
FROM (
    SELECT category, sum(value) as total
    FROM t_cache_subquery
    WHERE id < 2000
    GROUP BY category
    SETTINGS use_columns_cache = 1
)
ORDER BY category
SETTINGS use_columns_cache = 1;

-- Repeat subquery
SELECT category, total
FROM (
    SELECT category, sum(value) as total
    FROM t_cache_subquery
    WHERE id < 2000
    GROUP BY category
    SETTINGS use_columns_cache = 1
)
ORDER BY category
SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_subquery;

-- ============================================================================
-- Test 13: ALTER TABLE operations
-- ============================================================================

DROP TABLE IF EXISTS t_cache_alter;

CREATE TABLE t_cache_alter (id UInt64, value String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_alter SELECT number, toString(number) FROM numbers(3000);

SYSTEM DROP COLUMNS CACHE;

-- Populate cache
SELECT sum(id), count() FROM t_cache_alter SETTINGS use_columns_cache = 1;

-- Add column
ALTER TABLE t_cache_alter ADD COLUMN new_col UInt64 DEFAULT id * 10;

-- Read with new column (should produce correct results)
SELECT sum(id), sum(new_col), count() FROM t_cache_alter SETTINGS use_columns_cache = 1;

-- Repeat
SELECT sum(id), sum(new_col), count() FROM t_cache_alter SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_alter;

-- ============================================================================
-- Test 14: LowCardinality columns
-- ============================================================================

DROP TABLE IF EXISTS t_cache_lowcard;

CREATE TABLE t_cache_lowcard (
    id UInt64,
    lc_string LowCardinality(String),
    lc_nullable LowCardinality(Nullable(String))
) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_lowcard SELECT
    number,
    'category_' || toString(number % 100),
    if(number % 10 = 0, NULL, 'value_' || toString(number % 50))
FROM numbers(5000);

SYSTEM DROP COLUMNS CACHE;

-- Read LowCardinality columns
SELECT sum(id), count(DISTINCT lc_string), count(lc_nullable), count() FROM t_cache_lowcard
SETTINGS use_columns_cache = 1;

-- Repeat
SELECT sum(id), count(DISTINCT lc_string), count(lc_nullable), count() FROM t_cache_lowcard
SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_lowcard;

-- ============================================================================
-- Test 15: Concurrent inserts and reads
-- ============================================================================

DROP TABLE IF EXISTS t_cache_concurrent;

CREATE TABLE t_cache_concurrent (id UInt64, value UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_concurrent SELECT number, number * 5 FROM numbers(3000);

SYSTEM DROP COLUMNS CACHE;

-- Populate cache
SELECT sum(id), count() FROM t_cache_concurrent SETTINGS use_columns_cache = 1;

-- Insert new data (creates new part)
INSERT INTO t_cache_concurrent SELECT number + 3000, (number + 3000) * 5 FROM numbers(3000);

-- Read all data (should use cache for old part, read new part)
SELECT sum(id), count() FROM t_cache_concurrent SETTINGS use_columns_cache = 1;

-- Repeat (should use cache for both parts)
SELECT sum(id), count() FROM t_cache_concurrent SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_concurrent;

-- Final message
SELECT 'All correctness tests passed' as result;
