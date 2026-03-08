-- Test columns cache with various intersecting ranges and String subcolumn optimization
-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET use_columns_cache = 1;
SET enable_reads_from_columns_cache = 1;
SET enable_writes_to_columns_cache = 1;

DROP TABLE IF EXISTS t_cache_ranges;

-- Create table with enough data to span multiple granules
CREATE TABLE t_cache_ranges (
    id UInt64,
    value String,
    number UInt64
) ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

-- Insert 10000 rows (10 granules with granularity 1000)
INSERT INTO t_cache_ranges
SELECT
    number AS id,
    'value_' || toString(number) AS value,
    number * 2 AS number
FROM numbers(10000);

SYSTEM DROP COLUMNS CACHE;

-- =============================================================================
-- Test 1: Read range covered by cached range
-- First read: [2000, 8000) - marks 2-8
-- Second read: [3000, 5000) - marks 3-5 (subset of first read)
-- =============================================================================

SELECT 'Test 1: Read range covered by cached range';

-- First read: cache marks 2-8
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 2000 AND id < 8000;

-- Second read: should be fully served from cache (marks 3-5 are cached)
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 3000 AND id < 5000;

-- Verify by reading again
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 3000 AND id < 5000;

-- =============================================================================
-- Test 2: Read range covering cached range
-- First read: [4000, 6000) - marks 4-6
-- Second read: [2000, 8000) - marks 2-8 (superset of first read)
-- =============================================================================

SYSTEM DROP COLUMNS CACHE;

SELECT 'Test 2: Read range covering cached range';

-- First read: cache marks 4-6
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 4000 AND id < 6000;

-- Second read: marks 4-6 from cache, marks 2-4 and 6-8 from disk
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 2000 AND id < 8000;

-- Verify entire range is now cached
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 2000 AND id < 8000;

-- =============================================================================
-- Test 3: Partial intersection - left overlap
-- First read: [3000, 7000) - marks 3-7
-- Second read: [1000, 5000) - marks 1-5 (overlaps on left side)
-- =============================================================================

SYSTEM DROP COLUMNS CACHE;

SELECT 'Test 3: Partial intersection - left overlap';

-- First read: cache marks 3-7
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 3000 AND id < 7000;

-- Second read: marks 3-5 from cache, marks 1-3 from disk
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 1000 AND id < 5000;

-- Verify both ranges are now cached
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 1000 AND id < 5000;
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 3000 AND id < 7000;

-- =============================================================================
-- Test 4: Partial intersection - right overlap
-- First read: [2000, 5000) - marks 2-5
-- Second read: [4000, 8000) - marks 4-8 (overlaps on right side)
-- =============================================================================

SYSTEM DROP COLUMNS CACHE;

SELECT 'Test 4: Partial intersection - right overlap';

-- First read: cache marks 2-5
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 2000 AND id < 5000;

-- Second read: marks 4-5 from cache, marks 5-8 from disk
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 4000 AND id < 8000;

-- Verify both ranges are now cached
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 2000 AND id < 5000;
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 4000 AND id < 8000;

-- =============================================================================
-- Test 5: Non-overlapping ranges
-- First read: [1000, 3000) - marks 1-3
-- Second read: [7000, 9000) - marks 7-9 (no overlap)
-- =============================================================================

SYSTEM DROP COLUMNS CACHE;

SELECT 'Test 5: Non-overlapping ranges';

-- First read: cache marks 1-3
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 1000 AND id < 3000;

-- Second read: completely from disk (no overlap)
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 7000 AND id < 9000;

-- Verify both ranges are cached independently
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 1000 AND id < 3000;
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 7000 AND id < 9000;

-- =============================================================================
-- Test 6: Multiple small ranges merging into larger range
-- First: [2000, 3000), [5000, 6000), [8000, 9000)
-- Then: [2000, 9000) - should use cached parts where available
-- =============================================================================

SYSTEM DROP COLUMNS CACHE;

SELECT 'Test 6: Multiple small ranges merging';

-- Cache three separate ranges
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 2000 AND id < 3000;
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 5000 AND id < 6000;
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 8000 AND id < 9000;

-- Read spanning range: should use cache for marks 2-3, 5-6, 8-9
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 2000 AND id < 9000;

-- Verify entire range is now cached
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 2000 AND id < 9000;

-- =============================================================================
-- Test 7: String subcolumn optimization - read subcolumn then full string
-- =============================================================================

DROP TABLE IF EXISTS t_cache_string_subcolumns;

CREATE TABLE t_cache_string_subcolumns (
    id UInt64,
    s String
) ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_cache_string_subcolumns
SELECT
    number AS id,
    'string_value_' || toString(number) || '_with_some_extra_data' AS s
FROM numbers(5000);

SYSTEM DROP COLUMNS CACHE;

SELECT 'Test 7: Read String.size subcolumn first, then full String';

-- First read: String.size subcolumn only (optimized - reads only offsets)
SELECT count(), sum(length(s)) FROM t_cache_string_subcolumns WHERE id < 3000;

-- Second read: full String column (needs both offsets and data)
-- The offsets should be served from cache if cached separately
SELECT count(), sum(length(s)), any(substring(s, 1, 13)) FROM t_cache_string_subcolumns WHERE id < 3000;

-- Third read: verify full String is now cached
SELECT count(), sum(length(s)), any(substring(s, 1, 13)) FROM t_cache_string_subcolumns WHERE id < 3000;

-- =============================================================================
-- Test 8: Base column caching - verify full String is cached and reused
-- =============================================================================

SYSTEM DROP COLUMNS CACHE;

SELECT 'Test 8: Read full String and verify caching';

-- First read: full String column (caches "s")
SELECT count(), sum(length(s)), any(substring(s, 1, 13)) FROM t_cache_string_subcolumns WHERE id >= 3000;

-- Second read: full String again (served from cache)
SELECT count(), sum(length(s)), any(substring(s, 1, 13)) FROM t_cache_string_subcolumns WHERE id >= 3000;

-- Third read: verify still served from cache
SELECT count(), sum(length(s)), any(substring(s, 1, 13)) FROM t_cache_string_subcolumns WHERE id >= 3000;

-- =============================================================================
-- Test 9: Subcolumn caching with range intersections
-- =============================================================================

SYSTEM DROP COLUMNS CACHE;

SELECT 'Test 9: String subcolumn with intersecting ranges';

-- Read String.size for range [1000, 4000) - caches "s.size"
SELECT count(), sum(length(s)) FROM t_cache_string_subcolumns WHERE id >= 1000 AND id < 4000;

-- Read full String for overlapping range [2000, 5000) - reads from disk, caches "s"
SELECT count(), any(substring(s, 1, 13)) FROM t_cache_string_subcolumns WHERE id >= 2000 AND id < 5000;

-- Verify s.size still cached for original range
SELECT count(), sum(length(s)) FROM t_cache_string_subcolumns WHERE id >= 1000 AND id < 4000;

-- Verify full String cached for its range
SELECT count(), any(substring(s, 1, 13)) FROM t_cache_string_subcolumns WHERE id >= 2000 AND id < 5000;

-- =============================================================================
-- Test 10: Adjacent ranges
-- First read: [3000, 5000) - marks 3-5
-- Second read: [5000, 7000) - marks 5-7 (adjacent, no overlap)
-- Third read: [3000, 7000) - marks 3-7 (should combine both cached ranges)
-- =============================================================================

DROP TABLE t_cache_string_subcolumns;

SYSTEM DROP COLUMNS CACHE;

SELECT 'Test 10: Adjacent ranges';

-- First read: cache marks 3-5
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 3000 AND id < 5000;

-- Second read: cache marks 5-7 (adjacent to previous)
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 5000 AND id < 7000;

-- Third read: should use both cached ranges for marks 3-7
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 3000 AND id < 7000;

-- Verify
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 3000 AND id < 7000;

DROP TABLE t_cache_ranges;

SELECT 'All range intersection and subcolumn tests passed';
