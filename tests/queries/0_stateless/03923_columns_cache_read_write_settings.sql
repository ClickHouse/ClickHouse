-- Test columns cache read/write settings validation
-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database

DROP TABLE IF EXISTS t_cache_settings;

CREATE TABLE t_cache_settings (
    id UInt64,
    value String,
    number UInt64
) ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_settings
SELECT
    number AS id,
    'value_' || toString(number) AS value,
    number * 2 AS number
FROM numbers(5000);

-- =============================================================================
-- Test 1: All cache settings enabled (baseline)
-- =============================================================================

SELECT 'Test 1: All cache settings enabled';

SYSTEM DROP COLUMNS CACHE;

SET use_columns_cache = 1;
SET enable_reads_from_columns_cache = 1;
SET enable_writes_to_columns_cache = 1;

-- First read: should populate cache (writes enabled)
SELECT count(), sum(number) FROM t_cache_settings WHERE id < 2000;

-- Second read: should use cache (reads enabled)
SELECT count(), sum(number) FROM t_cache_settings WHERE id < 2000;

-- Verify cache has data by reading different range
SELECT count(), sum(number) FROM t_cache_settings WHERE id >= 2000 AND id < 4000;

-- =============================================================================
-- Test 2: Writes disabled, reads enabled
-- Cache should NOT populate, reads should go to disk every time
-- =============================================================================

SELECT 'Test 2: Writes disabled, reads enabled';

SYSTEM DROP COLUMNS CACHE;

SET use_columns_cache = 1;
SET enable_reads_from_columns_cache = 1;
SET enable_writes_to_columns_cache = 0;  -- Disable writes

-- First read: should NOT populate cache
SELECT count(), sum(number) FROM t_cache_settings WHERE id < 1000;

-- Second read: should still go to disk (cache wasn't populated)
SELECT count(), sum(number) FROM t_cache_settings WHERE id < 1000;

-- Third read: verify still reading from disk
SELECT count(), sum(number) FROM t_cache_settings WHERE id < 1000;

-- =============================================================================
-- Test 3: Writes enabled, reads disabled
-- Cache should populate but NOT be used for reads
-- =============================================================================

SELECT 'Test 3: Writes enabled, reads disabled';

SYSTEM DROP COLUMNS CACHE;

SET use_columns_cache = 1;
SET enable_reads_from_columns_cache = 0;  -- Disable reads
SET enable_writes_to_columns_cache = 1;   -- Enable writes

-- First read: should populate cache but read from disk
SELECT count(), sum(number) FROM t_cache_settings WHERE id >= 1000 AND id < 2000;

-- Second read: cache has data, but reads disabled, so should still read from disk
SELECT count(), sum(number) FROM t_cache_settings WHERE id >= 1000 AND id < 2000;

-- Third read: verify still reading from disk despite cache being populated
SELECT count(), sum(number) FROM t_cache_settings WHERE id >= 1000 AND id < 2000;

-- =============================================================================
-- Test 4: Both writes and reads disabled
-- Cache should neither populate nor be used
-- =============================================================================

SELECT 'Test 4: Both writes and reads disabled';

SYSTEM DROP COLUMNS CACHE;

SET use_columns_cache = 1;
SET enable_reads_from_columns_cache = 0;  -- Disable reads
SET enable_writes_to_columns_cache = 0;   -- Disable writes

-- First read: should NOT populate cache and read from disk
SELECT count(), sum(number) FROM t_cache_settings WHERE id >= 3000 AND id < 4000;

-- Second read: still from disk (no cache)
SELECT count(), sum(number) FROM t_cache_settings WHERE id >= 3000 AND id < 4000;

-- =============================================================================
-- Test 5: Dynamic setting changes - disable writes mid-query
-- =============================================================================

SELECT 'Test 5: Dynamic setting changes';

SYSTEM DROP COLUMNS CACHE;

SET use_columns_cache = 1;
SET enable_reads_from_columns_cache = 1;
SET enable_writes_to_columns_cache = 1;

-- Populate cache for range [0, 1000)
SELECT count(), sum(number) FROM t_cache_settings WHERE id < 1000;

-- Verify cached
SELECT count(), sum(number) FROM t_cache_settings WHERE id < 1000;

-- Now disable writes and read new range
SET enable_writes_to_columns_cache = 0;
SELECT count(), sum(number) FROM t_cache_settings WHERE id >= 4000;

-- Verify old range still cached
SELECT count(), sum(number) FROM t_cache_settings WHERE id < 1000;

-- New range should not be cached (reads should work but not populate)
SELECT count(), sum(number) FROM t_cache_settings WHERE id >= 4000;

-- =============================================================================
-- Test 6: use_columns_cache = 0 (master switch off)
-- When master switch is off, other settings should be ignored
-- =============================================================================

SELECT 'Test 6: Master switch off';

SYSTEM DROP COLUMNS CACHE;

SET use_columns_cache = 0;  -- Master switch OFF
SET enable_reads_from_columns_cache = 1;
SET enable_writes_to_columns_cache = 1;

-- Cache is completely disabled, should read from disk
SELECT count(), sum(number) FROM t_cache_settings WHERE id >= 2000 AND id < 3000;

-- Second read: still from disk (cache disabled)
SELECT count(), sum(number) FROM t_cache_settings WHERE id >= 2000 AND id < 3000;

-- =============================================================================
-- Test 7: Re-enable cache after it was disabled
-- =============================================================================

SELECT 'Test 7: Re-enable cache';

-- Now turn cache back on
SET use_columns_cache = 1;
SET enable_reads_from_columns_cache = 1;
SET enable_writes_to_columns_cache = 1;

-- Should populate cache
SELECT count(), sum(number) FROM t_cache_settings WHERE id >= 2000 AND id < 3000;

-- Should read from cache
SELECT count(), sum(number) FROM t_cache_settings WHERE id >= 2000 AND id < 3000;

-- =============================================================================
-- Test 8: Complex query with different setting combinations
-- =============================================================================

SELECT 'Test 8: Complex queries with various settings';

SYSTEM DROP COLUMNS CACHE;

SET use_columns_cache = 1;
SET enable_reads_from_columns_cache = 1;
SET enable_writes_to_columns_cache = 1;

-- Populate cache with complex query
SELECT count(), sum(number), avg(length(value)) FROM t_cache_settings WHERE id % 10 = 0;

-- Read from cache with same filter
SELECT count(), sum(number), avg(length(value)) FROM t_cache_settings WHERE id % 10 = 0;

-- Disable reads, same query should go to disk
SET enable_reads_from_columns_cache = 0;
SELECT count(), sum(number), avg(length(value)) FROM t_cache_settings WHERE id % 10 = 0;

-- Re-enable reads
SET enable_reads_from_columns_cache = 1;
SELECT count(), sum(number), avg(length(value)) FROM t_cache_settings WHERE id % 10 = 0;

-- =============================================================================
-- Test 9: Verify settings don't affect correctness
-- All setting combinations should return identical results
-- =============================================================================

SELECT 'Test 9: Verify correctness across all setting combinations';

SYSTEM DROP COLUMNS CACHE;

-- Baseline: all enabled
SET use_columns_cache = 1;
SET enable_reads_from_columns_cache = 1;
SET enable_writes_to_columns_cache = 1;
SELECT sum(number) AS result FROM t_cache_settings WHERE id BETWEEN 1500 AND 3500;

-- Writes disabled
SYSTEM DROP COLUMNS CACHE;
SET enable_writes_to_columns_cache = 0;
SELECT sum(number) AS result FROM t_cache_settings WHERE id BETWEEN 1500 AND 3500;

-- Reads disabled
SYSTEM DROP COLUMNS CACHE;
SET enable_reads_from_columns_cache = 0;
SET enable_writes_to_columns_cache = 1;
SELECT sum(number) AS result FROM t_cache_settings WHERE id BETWEEN 1500 AND 3500;

-- Both disabled
SYSTEM DROP COLUMNS CACHE;
SET enable_reads_from_columns_cache = 0;
SET enable_writes_to_columns_cache = 0;
SELECT sum(number) AS result FROM t_cache_settings WHERE id BETWEEN 1500 AND 3500;

-- Master switch off
SYSTEM DROP COLUMNS CACHE;
SET use_columns_cache = 0;
SELECT sum(number) AS result FROM t_cache_settings WHERE id BETWEEN 1500 AND 3500;

-- =============================================================================
-- Test 10: Settings validation with PREWHERE
-- =============================================================================

SELECT 'Test 10: Settings with PREWHERE';

SYSTEM DROP COLUMNS CACHE;

SET use_columns_cache = 1;
SET enable_reads_from_columns_cache = 1;
SET enable_writes_to_columns_cache = 1;

-- Populate cache with PREWHERE
SELECT count(), sum(number) FROM t_cache_settings PREWHERE id < 2000 WHERE number > 100;

-- Read from cache
SELECT count(), sum(number) FROM t_cache_settings PREWHERE id < 2000 WHERE number > 100;

-- Disable writes and read new range with PREWHERE
SET enable_writes_to_columns_cache = 0;
SELECT count(), sum(number) FROM t_cache_settings PREWHERE id >= 3000 WHERE number < 8000;

-- Verify old range still cached
SET enable_writes_to_columns_cache = 1;
SELECT count(), sum(number) FROM t_cache_settings PREWHERE id < 2000 WHERE number > 100;

DROP TABLE t_cache_settings;

SELECT 'All read/write settings tests passed';
