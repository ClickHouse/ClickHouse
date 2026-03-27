-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- Edge cases and stress tests for columns cache

SET max_threads = 1; -- Ensure deterministic read order for cache testing
SET use_columns_cache = 1;
SET enable_reads_from_columns_cache = 1;
SET enable_writes_to_columns_cache = 1;
SYSTEM DROP COLUMNS CACHE;

-- ============================================================================
-- Test 1: Very small granules and mark ranges
-- ============================================================================

DROP TABLE IF EXISTS t_cache_small_granules;

CREATE TABLE t_cache_small_granules (id UInt64, value String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 100;

INSERT INTO t_cache_small_granules SELECT number, toString(number) FROM numbers(1000);

SYSTEM DROP COLUMNS CACHE;

-- Read with very small mark ranges
SELECT sum(id), count() FROM t_cache_small_granules WHERE id % 100 = 0 SETTINGS use_columns_cache = 1;
SELECT sum(id), count() FROM t_cache_small_granules WHERE id % 100 = 0 SETTINGS use_columns_cache = 1;

-- Read single row
SELECT id, value FROM t_cache_small_granules WHERE id = 500 SETTINGS use_columns_cache = 1;
SELECT id, value FROM t_cache_small_granules WHERE id = 500 SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_small_granules;

-- ============================================================================
-- Test 2: Very large granules
-- ============================================================================

DROP TABLE IF EXISTS t_cache_large_granules;

CREATE TABLE t_cache_large_granules (id UInt64, value String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 65536;

INSERT INTO t_cache_large_granules SELECT number, toString(number) FROM numbers(100000);

SYSTEM DROP COLUMNS CACHE;

-- Read with large granules
SELECT sum(id), count() FROM t_cache_large_granules SETTINGS use_columns_cache = 1;
SELECT sum(id), count() FROM t_cache_large_granules SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_large_granules;

-- ============================================================================
-- Test 3: Cache with ORDER BY and LIMIT
-- ============================================================================

DROP TABLE IF EXISTS t_cache_order_limit;

CREATE TABLE t_cache_order_limit (id UInt64, value UInt64, extra String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_order_limit SELECT number, (number * 7919) % 1000, toString(number) FROM numbers(10000);

SYSTEM DROP COLUMNS CACHE;

-- Query with ORDER BY and LIMIT
SELECT id, value FROM t_cache_order_limit ORDER BY value, id LIMIT 10 SETTINGS use_columns_cache = 1;
SELECT id, value FROM t_cache_order_limit ORDER BY value, id LIMIT 10 SETTINGS use_columns_cache = 1;

-- Query with different LIMIT
SELECT id, value FROM t_cache_order_limit ORDER BY value, id LIMIT 100 SETTINGS use_columns_cache = 1;
SELECT id, value FROM t_cache_order_limit ORDER BY value, id LIMIT 100 SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_order_limit;

-- ============================================================================
-- Test 4: Aggregation with GROUP BY
-- ============================================================================

DROP TABLE IF EXISTS t_cache_groupby;

CREATE TABLE t_cache_groupby (
    id UInt64,
    category UInt32,
    subcategory UInt32,
    value UInt64
) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_groupby SELECT
    number,
    number % 100,
    number % 10,
    number * 7
FROM numbers(20000);

SYSTEM DROP COLUMNS CACHE;

-- GROUP BY with cache
SELECT category, sum(value), count() FROM t_cache_groupby
GROUP BY category
ORDER BY category
LIMIT 5
SETTINGS use_columns_cache = 1;

-- Repeat
SELECT category, sum(value), count() FROM t_cache_groupby
GROUP BY category
ORDER BY category
LIMIT 5
SETTINGS use_columns_cache = 1;

-- Multi-level GROUP BY
SELECT category, subcategory, sum(value) FROM t_cache_groupby
GROUP BY category, subcategory
ORDER BY category, subcategory
LIMIT 10
SETTINGS use_columns_cache = 1;

-- Repeat
SELECT category, subcategory, sum(value) FROM t_cache_groupby
GROUP BY category, subcategory
ORDER BY category, subcategory
LIMIT 10
SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_groupby;

-- ============================================================================
-- Test 5: DISTINCT with cache
-- ============================================================================

DROP TABLE IF EXISTS t_cache_distinct;

CREATE TABLE t_cache_distinct (id UInt64, category String, value UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_distinct SELECT
    number,
    'cat_' || toString(number % 50),
    number % 100
FROM numbers(5000);

SYSTEM DROP COLUMNS CACHE;

-- DISTINCT with cache
SELECT count(DISTINCT category), count(DISTINCT value) FROM t_cache_distinct
SETTINGS use_columns_cache = 1;

-- Repeat
SELECT count(DISTINCT category), count(DISTINCT value) FROM t_cache_distinct
SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_distinct;

-- ============================================================================
-- Test 6: Index skip optimization with cache
-- ============================================================================

DROP TABLE IF EXISTS t_cache_skip_index;

CREATE TABLE t_cache_skip_index (
    id UInt64,
    category UInt32,
    value String,
    INDEX idx_category category TYPE minmax GRANULARITY 3
) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_skip_index SELECT
    number,
    number % 100,
    toString(number)
FROM numbers(10000);

SYSTEM DROP COLUMNS CACHE;

-- Query using skip index
SELECT sum(id), count() FROM t_cache_skip_index
WHERE category = 42
SETTINGS use_columns_cache = 1;

-- Repeat
SELECT sum(id), count() FROM t_cache_skip_index
WHERE category = 42
SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_skip_index;

-- ============================================================================
-- Test 7: NULL values and sparse columns
-- ============================================================================

DROP TABLE IF EXISTS t_cache_nulls;

CREATE TABLE t_cache_nulls (
    id UInt64,
    nullable_int Nullable(UInt64),
    nullable_string Nullable(String),
    arr Array(Nullable(UInt64))
) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_nulls SELECT
    number,
    if(number % 5 = 0, NULL, number),
    if(number % 7 = 0, NULL, toString(number)),
    if(number % 3 = 0, [NULL], [number, number + 1])
FROM numbers(5000);

SYSTEM DROP COLUMNS CACHE;

-- Read with many NULLs
SELECT
    sum(id),
    sum(nullable_int),
    count(nullable_string),
    count(arr),
    count()
FROM t_cache_nulls
SETTINGS use_columns_cache = 1;

-- Repeat
SELECT
    sum(id),
    sum(nullable_int),
    count(nullable_string),
    count(arr),
    count()
FROM t_cache_nulls
SETTINGS use_columns_cache = 1;

-- Filter on NULL
SELECT sum(id), count() FROM t_cache_nulls
WHERE nullable_int IS NULL
SETTINGS use_columns_cache = 1;

-- Repeat
SELECT sum(id), count() FROM t_cache_nulls
WHERE nullable_int IS NULL
SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_nulls;

-- ============================================================================
-- Test 8: Expression columns and virtual columns
-- ============================================================================

DROP TABLE IF EXISTS t_cache_expressions;

CREATE TABLE t_cache_expressions (
    id UInt64,
    value UInt64,
    calculated UInt64 MATERIALIZED value * 2,
    aliased UInt64 ALIAS value + id
) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_expressions (id, value) SELECT number, number * 3 FROM numbers(5000);

SYSTEM DROP COLUMNS CACHE;

-- Read with materialized and alias columns
SELECT sum(id), sum(value), sum(calculated), sum(aliased), count() FROM t_cache_expressions
SETTINGS use_columns_cache = 1;

-- Repeat
SELECT sum(id), sum(value), sum(calculated), sum(aliased), count() FROM t_cache_expressions
SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_expressions;

-- ============================================================================
-- Test 9: Multiple simultaneous queries (sequential simulation)
-- ============================================================================

DROP TABLE IF EXISTS t_cache_multi_query;

CREATE TABLE t_cache_multi_query (id UInt64, value UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_multi_query SELECT number, number * 11 FROM numbers(8000);

SYSTEM DROP COLUMNS CACHE;

-- Simulate multiple different queries
SELECT sum(id) FROM t_cache_multi_query WHERE id < 2000 SETTINGS use_columns_cache = 1;
SELECT sum(value) FROM t_cache_multi_query WHERE id < 3000 SETTINGS use_columns_cache = 1;
SELECT count() FROM t_cache_multi_query WHERE id < 4000 SETTINGS use_columns_cache = 1;
SELECT sum(id), sum(value) FROM t_cache_multi_query WHERE id < 5000 SETTINGS use_columns_cache = 1;

-- Repeat all queries (should hit cache)
SELECT sum(id) FROM t_cache_multi_query WHERE id < 2000 SETTINGS use_columns_cache = 1;
SELECT sum(value) FROM t_cache_multi_query WHERE id < 3000 SETTINGS use_columns_cache = 1;
SELECT count() FROM t_cache_multi_query WHERE id < 4000 SETTINGS use_columns_cache = 1;
SELECT sum(id), sum(value) FROM t_cache_multi_query WHERE id < 5000 SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_multi_query;

-- ============================================================================
-- Test 10: Default values
-- ============================================================================

DROP TABLE IF EXISTS t_cache_defaults;

CREATE TABLE t_cache_defaults (
    id UInt64,
    col_with_default UInt64 DEFAULT 999,
    col_with_expression String DEFAULT concat('prefix_', toString(id))
) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_defaults (id) SELECT number FROM numbers(3000);

SYSTEM DROP COLUMNS CACHE;

-- Read with default values
SELECT sum(id), sum(col_with_default), count() FROM t_cache_defaults
SETTINGS use_columns_cache = 1;

-- Repeat
SELECT sum(id), sum(col_with_default), count() FROM t_cache_defaults
SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_defaults;

-- ============================================================================
-- Test 11: TTL columns (without actual TTL expiration)
-- ============================================================================

DROP TABLE IF EXISTS t_cache_ttl;

CREATE TABLE t_cache_ttl (
    id UInt64,
    value String,
    timestamp DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY id
TTL timestamp + INTERVAL 1 YEAR
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_ttl (id, value) SELECT number, toString(number) FROM numbers(3000);

SYSTEM DROP COLUMNS CACHE;

-- Read table with TTL
SELECT sum(id), count() FROM t_cache_ttl SETTINGS use_columns_cache = 1;

-- Repeat
SELECT sum(id), count() FROM t_cache_ttl SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_ttl;

-- ============================================================================
-- Test 12: Primary key columns
-- ============================================================================

DROP TABLE IF EXISTS t_cache_primary_key;

CREATE TABLE t_cache_primary_key (
    pk1 UInt64,
    pk2 String,
    value UInt64
) ENGINE = MergeTree
ORDER BY (pk1, pk2)
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_primary_key SELECT
    number % 100,
    toString(number % 50),
    number
FROM numbers(10000);

SYSTEM DROP COLUMNS CACHE;

-- Read primary key columns
SELECT sum(pk1), count(DISTINCT pk2), sum(value) FROM t_cache_primary_key
SETTINGS use_columns_cache = 1;

-- Repeat
SELECT sum(pk1), count(DISTINCT pk2), sum(value) FROM t_cache_primary_key
SETTINGS use_columns_cache = 1;

-- Query using primary key
SELECT sum(value) FROM t_cache_primary_key WHERE pk1 = 42
SETTINGS use_columns_cache = 1;

-- Repeat
SELECT sum(value) FROM t_cache_primary_key WHERE pk1 = 42
SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_primary_key;

-- ============================================================================
-- Test 13: FINAL modifier with ReplacingMergeTree
-- ============================================================================

DROP TABLE IF EXISTS t_cache_replacing;

CREATE TABLE t_cache_replacing (
    id UInt64,
    value UInt64,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_replacing SELECT number, number * 2, 1 FROM numbers(2000);
INSERT INTO t_cache_replacing SELECT number, number * 3, 2 FROM numbers(1000);

SYSTEM DROP COLUMNS CACHE;

-- Read without FINAL
SELECT sum(id), sum(value), count() FROM t_cache_replacing
SETTINGS use_columns_cache = 1;

-- Repeat
SELECT sum(id), sum(value), count() FROM t_cache_replacing
SETTINGS use_columns_cache = 1;

-- Read with FINAL
SELECT sum(id), sum(value), count() FROM t_cache_replacing FINAL
SETTINGS use_columns_cache = 1;

-- Repeat
SELECT sum(id), sum(value), count() FROM t_cache_replacing FINAL
SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_replacing;

-- ============================================================================
-- Test 14: SummingMergeTree
-- ============================================================================

DROP TABLE IF EXISTS t_cache_summing;

CREATE TABLE t_cache_summing (
    id UInt64,
    category UInt32,
    value UInt64
) ENGINE = SummingMergeTree(value)
ORDER BY (id, category)
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_summing SELECT number % 100, number % 10, number FROM numbers(5000);

SYSTEM DROP COLUMNS CACHE;

-- Read SummingMergeTree
SELECT sum(id), sum(value), count() FROM t_cache_summing
SETTINGS use_columns_cache = 1;

-- Repeat
SELECT sum(id), sum(value), count() FROM t_cache_summing
SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_summing;

-- ============================================================================
-- Test 15: Mix of compact and wide parts
-- ============================================================================

DROP TABLE IF EXISTS t_cache_mixed_parts;

CREATE TABLE t_cache_mixed_parts (id UInt64, value String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 10485760, index_granularity = 1000;

-- Insert small data (compact parts)
INSERT INTO t_cache_mixed_parts SELECT number, toString(number) FROM numbers(100);

-- Insert larger data (might be wide parts)
INSERT INTO t_cache_mixed_parts SELECT number + 1000, repeat(toString(number), 100) FROM numbers(2000);

SYSTEM DROP COLUMNS CACHE;

-- Read mixed parts
SELECT sum(id), count() FROM t_cache_mixed_parts SETTINGS use_columns_cache = 1;

-- Repeat
SELECT sum(id), count() FROM t_cache_mixed_parts SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_mixed_parts;

SELECT 'All edge case tests passed' as result;
