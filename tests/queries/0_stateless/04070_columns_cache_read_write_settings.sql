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
SET log_queries = 1;

-- First read: should populate cache (writes enabled)
SELECT count(), sum(number) FROM t_cache_settings WHERE id < 2000;

-- Second read: should use cache (reads enabled)
SELECT count(), sum(number) FROM t_cache_settings WHERE id < 2000
SETTINGS log_comment = '04070_test1_read2';

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

-- With writes disabled the reads above must not have populated the cache.
SELECT count() FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cache_settings';

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

-- With writes enabled the read above must have populated the cache even though reads are disabled.
SELECT count() > 0 FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cache_settings';

-- Second read: cache has data, but reads disabled, so should still read from disk
SELECT count(), sum(number) FROM t_cache_settings WHERE id >= 1000 AND id < 2000
SETTINGS log_comment = '04070_test3_read2';

-- Third read: verify still reading from disk despite cache being populated
SELECT count(), sum(number) FROM t_cache_settings WHERE id >= 1000 AND id < 2000
SETTINGS log_comment = '04070_test3_read3';

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

-- The read above must have observed the change of the setting, and not only returned the same
-- numbers: the rows of the new range must not have entered the cache, while the range cached
-- before the change stays there.
SELECT count() FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_cache_settings' AND row_end > 4000;
SELECT count() > 0 FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_cache_settings' AND row_begin = 0;

-- Verify old range still cached
SELECT count(), sum(number) FROM t_cache_settings WHERE id < 1000
SETTINGS log_comment = '04070_test5_cached_read';

-- New range should not be cached (reads should work but not populate)
SELECT count(), sum(number) FROM t_cache_settings WHERE id >= 4000
SETTINGS log_comment = '04070_test5_uncached_read';

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
SELECT count(), sum(number) FROM t_cache_settings WHERE id >= 2000 AND id < 3000
SETTINGS log_comment = '04070_test6_read2';

-- Observe that the master switch is really enforced, not only that the results are the same:
-- the reads above must have left nothing of this table in the cache, and the repeated read
-- must not have been served from it (checked through `ColumnsCacheHits` below).
SELECT count() FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cache_settings';

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

-- Disable reads, same query should go to disk. The `ColumnsCacheHits` of these two queries,
-- checked at the end of the test, show that the change of the setting was observed by the read
-- and not only that the numbers stayed the same.
SET enable_reads_from_columns_cache = 0;
SELECT count(), sum(number), avg(length(value)) FROM t_cache_settings WHERE id % 10 = 0
SETTINGS log_comment = '04070_test8_reads_disabled';

-- Re-enable reads
SET enable_reads_from_columns_cache = 1;
SELECT count(), sum(number), avg(length(value)) FROM t_cache_settings WHERE id % 10 = 0
SETTINGS log_comment = '04070_test8_reads_reenabled';

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

-- Same oracle as in Test 5, for the `PREWHERE` shape: the rows read after the change must not
-- have entered the cache, while the range cached before it stays there.
SELECT count() FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_cache_settings' AND row_begin >= 3000;
SELECT count() > 0 FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_cache_settings' AND row_begin < 2000;

-- Verify old range still cached
SET enable_writes_to_columns_cache = 1;
SELECT count(), sum(number) FROM t_cache_settings PREWHERE id < 2000 WHERE number > 100
SETTINGS log_comment = '04070_test10_cached_read';

DROP TABLE t_cache_settings;

-- =============================================================================
-- Verify through ProfileEvents that the settings actually changed behavior:
-- with reads enabled the repeated read of Test 1 was served from the cache,
-- and with reads disabled the repeated reads of Test 3 never touched it.
-- The same for the settings changed in the middle of the session: the reads of Test 5 and
-- Test 10 that follow the change tell a cached range from a range the change kept out of the
-- cache, and the two reads of Test 8 differ only by `enable_reads_from_columns_cache`.
-- =============================================================================

SELECT 'ProfileEvents checks';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['ColumnsCacheHits'] > 0 AS has_hits
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment IN ('04070_test1_read2', '04070_test3_read2', '04070_test3_read3', '04070_test6_read2',
        '04070_test5_cached_read', '04070_test5_uncached_read',
        '04070_test8_reads_disabled', '04070_test8_reads_reenabled',
        '04070_test10_cached_read')
ORDER BY log_comment;

SELECT 'All read/write settings tests passed';
