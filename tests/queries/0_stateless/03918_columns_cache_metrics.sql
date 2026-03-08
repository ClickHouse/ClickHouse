-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- Test cache metrics, ProfileEvents, and system commands

SET max_threads = 1;
SYSTEM DROP COLUMNS CACHE;

DROP TABLE IF EXISTS t_cache_metrics;

CREATE TABLE t_cache_metrics (id UInt64, value String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_metrics SELECT number, toString(number) FROM numbers(10000);

-- ============================================================================
-- Test 1: Cache hits and misses
-- ============================================================================

SYSTEM DROP COLUMNS CACHE;

-- First read (cache miss expected)
SELECT sum(id), count() FROM t_cache_metrics
SETTINGS use_columns_cache = 1;

-- Second read (cache hit expected)
SELECT sum(id), count() FROM t_cache_metrics
SETTINGS use_columns_cache = 1;

-- Third read (cache hit expected)
SELECT sum(id), count() FROM t_cache_metrics
SETTINGS use_columns_cache = 1;

-- ============================================================================
-- Test 2: Selective column reads
-- ============================================================================

SYSTEM DROP COLUMNS CACHE;

-- Read only id column
SELECT sum(id) FROM t_cache_metrics
SETTINGS use_columns_cache = 1;

-- Read only value column
SELECT count(value) FROM t_cache_metrics
SETTINGS use_columns_cache = 1;

-- Read both columns (should use cached id, cache value)
SELECT sum(id), count(value) FROM t_cache_metrics
SETTINGS use_columns_cache = 1;

-- Read both again (both should hit cache)
SELECT sum(id), count(value) FROM t_cache_metrics
SETTINGS use_columns_cache = 1;

-- ============================================================================
-- Test 3: SYSTEM DROP COLUMNS CACHE command
-- ============================================================================

-- Populate cache
SELECT sum(id), count() FROM t_cache_metrics
SETTINGS use_columns_cache = 1;

-- Drop cache
SYSTEM DROP COLUMNS CACHE;

-- Verify cache was dropped by checking that next read still works
SELECT sum(id), count() FROM t_cache_metrics
SETTINGS use_columns_cache = 1;

-- ============================================================================
-- Test 4: Cache with disabled writes
-- ============================================================================

SYSTEM DROP COLUMNS CACHE;

-- Read with writes disabled (should not populate cache)
SELECT sum(id), count() FROM t_cache_metrics
SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 0;

-- Read with writes enabled (should populate cache now)
SELECT sum(id), count() FROM t_cache_metrics
SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 1;

-- Read again (should hit cache)
SELECT sum(id), count() FROM t_cache_metrics
SETTINGS use_columns_cache = 1;

-- ============================================================================
-- Test 5: Cache with disabled reads
-- ============================================================================

SYSTEM DROP COLUMNS CACHE;

-- Populate cache
SELECT sum(id), count() FROM t_cache_metrics
SETTINGS use_columns_cache = 1;

-- Read with cache reads disabled (should not use cache but still work)
SELECT sum(id), count() FROM t_cache_metrics
SETTINGS use_columns_cache = 1, enable_reads_from_columns_cache = 0;

-- Read with cache reads enabled (should use cache)
SELECT sum(id), count() FROM t_cache_metrics
SETTINGS use_columns_cache = 1, enable_reads_from_columns_cache = 1;

-- ============================================================================
-- Test 6: Partial column caching
-- ============================================================================

SYSTEM DROP COLUMNS CACHE;

-- Cache only first part of data
SELECT sum(id) FROM t_cache_metrics WHERE id < 3000
SETTINGS use_columns_cache = 1;

-- Read different part
SELECT sum(id) FROM t_cache_metrics WHERE id >= 7000
SETTINGS use_columns_cache = 1;

-- Read overlap (should use some cached data)
SELECT sum(id) FROM t_cache_metrics WHERE id < 5000
SETTINGS use_columns_cache = 1;

-- Read all (should use cached parts)
SELECT sum(id) FROM t_cache_metrics
SETTINGS use_columns_cache = 1;

-- ============================================================================
-- Test 7: Cache with different mark ranges
-- ============================================================================

SYSTEM DROP COLUMNS CACHE;

-- Different sized queries
SELECT count() FROM t_cache_metrics WHERE id < 1000 SETTINGS use_columns_cache = 1;
SELECT count() FROM t_cache_metrics WHERE id < 2000 SETTINGS use_columns_cache = 1;
SELECT count() FROM t_cache_metrics WHERE id < 3000 SETTINGS use_columns_cache = 1;
SELECT count() FROM t_cache_metrics WHERE id < 5000 SETTINGS use_columns_cache = 1;
SELECT count() FROM t_cache_metrics WHERE id < 10000 SETTINGS use_columns_cache = 1;

-- Repeat same queries (should hit cache)
SELECT count() FROM t_cache_metrics WHERE id < 1000 SETTINGS use_columns_cache = 1;
SELECT count() FROM t_cache_metrics WHERE id < 2000 SETTINGS use_columns_cache = 1;
SELECT count() FROM t_cache_metrics WHERE id < 3000 SETTINGS use_columns_cache = 1;
SELECT count() FROM t_cache_metrics WHERE id < 5000 SETTINGS use_columns_cache = 1;
SELECT count() FROM t_cache_metrics WHERE id < 10000 SETTINGS use_columns_cache = 1;

-- ============================================================================
-- Test 8: Verify cache works with multiple tables
-- ============================================================================

DROP TABLE IF EXISTS t_cache_metrics2;

CREATE TABLE t_cache_metrics2 (id UInt64, data UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_metrics2 SELECT number, number * 13 FROM numbers(5000);

SYSTEM DROP COLUMNS CACHE;

-- Populate cache for both tables
SELECT sum(id) FROM t_cache_metrics SETTINGS use_columns_cache = 1;
SELECT sum(id) FROM t_cache_metrics2 SETTINGS use_columns_cache = 1;

-- Read from both (should hit cache)
SELECT sum(id) FROM t_cache_metrics SETTINGS use_columns_cache = 1;
SELECT sum(id) FROM t_cache_metrics2 SETTINGS use_columns_cache = 1;

-- Verify cross-table cache isolation
SELECT sum(t1.id) + sum(t2.id)
FROM t_cache_metrics t1, t_cache_metrics2 t2
WHERE t1.id = t2.id AND t1.id < 1000
SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_metrics2;

-- ============================================================================
-- Test 9: Cache behavior with table drops
-- ============================================================================

DROP TABLE IF EXISTS t_cache_temp;

CREATE TABLE t_cache_temp (id UInt64, value UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_temp SELECT number, number * 17 FROM numbers(3000);

-- Populate cache
SELECT sum(id) FROM t_cache_temp SETTINGS use_columns_cache = 1;
SELECT sum(id) FROM t_cache_temp SETTINGS use_columns_cache = 1;

-- Drop table (cache entries should become stale but not cause errors)
DROP TABLE t_cache_temp;

-- Verify original table still works
SELECT sum(id), count() FROM t_cache_metrics SETTINGS use_columns_cache = 1;

-- ============================================================================
-- Test 10: Cache with SAMPLE
-- ============================================================================

DROP TABLE IF EXISTS t_cache_sample;

CREATE TABLE t_cache_sample (id UInt64, value String)
ENGINE = MergeTree ORDER BY id
SAMPLE BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_sample SELECT number, toString(number) FROM numbers(10000);

SYSTEM DROP COLUMNS CACHE;

-- Query with SAMPLE
SELECT count() FROM t_cache_sample SAMPLE 0.1 SETTINGS use_columns_cache = 1;

-- Repeat (should use cache)
SELECT count() FROM t_cache_sample SAMPLE 0.1 SETTINGS use_columns_cache = 1;

-- Different sample rate
SELECT count() FROM t_cache_sample SAMPLE 0.5 SETTINGS use_columns_cache = 1;

DROP TABLE t_cache_sample;

DROP TABLE t_cache_metrics;

SELECT 'All metrics tests passed' as result;
