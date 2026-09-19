-- Test columns cache with various intersecting ranges and String subcolumn optimization
-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET use_columns_cache = 1;
SET enable_reads_from_columns_cache = 1;
SET enable_writes_to_columns_cache = 1;

-- The reads below are tagged with `log_comment` and their `ColumnsCacheHits` /
-- `ColumnsCacheMisses` counters are checked at the end of the test, so that a
-- cache lookup that always missed would be caught instead of silently falling
-- back to reading from disk with the same query result.
-- Pin `max_threads` so that the read is split into the same tasks - and therefore
-- produces the same number of cache lookups - regardless of the machine.
SET max_threads = 1;
SET log_queries = 1;

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
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 2000 AND id < 8000
SETTINGS log_comment = '04068_probe_subset_warm';

-- Second read: should be fully served from cache (marks 3-5 are cached)
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 3000 AND id < 5000
SETTINGS log_comment = '04068_probe_subset_covered';

-- Verify by reading again
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 3000 AND id < 5000
SETTINGS log_comment = '04068_probe_subset_covered_again';

-- =============================================================================
-- Test 2: Read range covering cached range
-- First read: [4000, 6000) - marks 4-6
-- Second read: [2000, 8000) - marks 2-8 (superset of first read)
-- =============================================================================

SYSTEM DROP COLUMNS CACHE;

SELECT 'Test 2: Read range covering cached range';

-- First read: cache marks 4-6
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 4000 AND id < 6000
SETTINGS log_comment = '04068_probe_superset_warm';

-- Second read: marks 4-6 from cache, marks 2-4 and 6-8 from disk
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 2000 AND id < 8000
SETTINGS log_comment = '04068_probe_superset_wider';

-- Verify entire range is now cached
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 2000 AND id < 8000
SETTINGS log_comment = '04068_probe_superset_wider_again';

-- =============================================================================
-- Test 3: Partial intersection - left overlap
-- First read: [3000, 7000) - marks 3-7
-- Second read: [1000, 5000) - marks 1-5 (overlaps on left side)
-- =============================================================================

SYSTEM DROP COLUMNS CACHE;

SELECT 'Test 3: Partial intersection - left overlap';

-- First read: cache marks 3-7
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 3000 AND id < 7000
SETTINGS log_comment = '04068_probe_left_overlap_warm';

-- Second read: marks 3-5 from cache, marks 1-3 from disk
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 1000 AND id < 5000
SETTINGS log_comment = '04068_probe_left_overlap_read';

-- The entries hold runs of granules of a stripe and adjacent runs are merged, so both
-- ranges are cached now and neither of the reads displaced the entries of the other:
-- both are served from the cache.
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 1000 AND id < 5000
SETTINGS log_comment = '04068_probe_left_overlap_read_again';
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 3000 AND id < 7000
SETTINGS log_comment = '04068_probe_left_overlap_warm_again';

-- =============================================================================
-- Test 4: Partial intersection - right overlap
-- First read: [2000, 5000) - marks 2-5
-- Second read: [4000, 8000) - marks 4-8 (overlaps on right side)
-- =============================================================================

SYSTEM DROP COLUMNS CACHE;

SELECT 'Test 4: Partial intersection - right overlap';

-- First read: cache marks 2-5
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 2000 AND id < 5000
SETTINGS log_comment = '04068_probe_right_overlap_warm';

-- Second read: marks 4-5 from cache, marks 5-8 from disk
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 4000 AND id < 8000
SETTINGS log_comment = '04068_probe_right_overlap_read';

-- Both ranges are cached at this point, merged into one run of granules, and both of
-- these reads are served from the cache.
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 2000 AND id < 5000
SETTINGS log_comment = '04068_probe_right_overlap_warm_again';
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 4000 AND id < 8000
SETTINGS log_comment = '04068_probe_right_overlap_read_again';

-- =============================================================================
-- Test 5: Non-overlapping ranges
-- First read: [1000, 3000) - marks 1-3
-- Second read: [7000, 9000) - marks 7-9 (no overlap)
-- =============================================================================

SYSTEM DROP COLUMNS CACHE;

SELECT 'Test 5: Non-overlapping ranges';

-- First read: cache marks 1-3
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 1000 AND id < 3000
SETTINGS log_comment = '04068_probe_disjoint_warm';

-- Second read: completely from disk (no overlap). This is the case that an
-- always-hitting intersection lookup would get wrong.
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 7000 AND id < 9000
SETTINGS log_comment = '04068_probe_disjoint_read';

-- The two ranges lie in the same stripe (the table is small enough for one stripe) with a gap
-- between them, and an entry holds one contiguous run of granules: only one of the two is kept,
-- the larger one, or the existing one when they are equal. The first range stays cached, the
-- second is read from disk again.
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 1000 AND id < 3000
SETTINGS log_comment = '04068_probe_disjoint_warm_again';
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 7000 AND id < 9000
SETTINGS log_comment = '04068_probe_disjoint_read_again';

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
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 2000 AND id < 9000
SETTINGS log_comment = '04068_probe_spanning_read';

-- Verify entire range is now cached
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 2000 AND id < 9000
SETTINGS log_comment = '04068_probe_spanning_read_again';

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
SELECT count(), sum(length(s)) FROM t_cache_string_subcolumns WHERE id >= 1000 AND id < 4000
SETTINGS log_comment = '04068_probe_subcolumn_size_warm';

-- Read full String for overlapping range [2000, 5000) - reads from disk, caches "s"
SELECT count(), any(substring(s, 1, 13)) FROM t_cache_string_subcolumns WHERE id >= 2000 AND id < 5000
SETTINGS log_comment = '04068_probe_subcolumn_full_read';

-- Verify s.size still cached for original range
SELECT count(), sum(length(s)) FROM t_cache_string_subcolumns WHERE id >= 1000 AND id < 4000
SETTINGS log_comment = '04068_probe_subcolumn_size_warm_again';

-- Verify full String cached for its range
SELECT count(), any(substring(s, 1, 13)) FROM t_cache_string_subcolumns WHERE id >= 2000 AND id < 5000
SETTINGS log_comment = '04068_probe_subcolumn_full_read_again';

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
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 3000 AND id < 5000
SETTINGS log_comment = '04068_probe_adjacent_left';

-- Second read: cache marks 5-7 (adjacent to previous)
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 5000 AND id < 7000
SETTINGS log_comment = '04068_probe_adjacent_right';

-- Third read: spans both cached ranges, which were merged into one entry, so the read
-- is served from the cache.
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 3000 AND id < 7000
SETTINGS log_comment = '04068_probe_adjacent_spanning';

-- Verify
SELECT count(), sum(number) FROM t_cache_ranges WHERE id >= 3000 AND id < 7000
SETTINGS log_comment = '04068_probe_adjacent_spanning_again';

DROP TABLE t_cache_ranges;

-- =============================================================================
-- The cache path of every case above, as observed through the per-query profile
-- events: `has_hits` tells that at least one granule of a column was served from the
-- cache, `has_misses` that at least one had to be read from disk.
--
-- An entry holds one contiguous run of granules of a stripe, and a read is served from the
-- cache for the granules that are there and reads the others from disk: a partially
-- overlapping or a spanning read both hits and misses, and the runs written by different reads
-- are merged when they overlap or touch rather than displacing each other - which is why
-- re-reading the older range hits in the left-overlap and right-overlap cases. Two runs with a
-- gap between them cannot both be kept, which is why the second disjoint range misses again.
-- =============================================================================

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['ColumnsCacheHits'] > 0 AS has_hits,
    ProfileEvents['ColumnsCacheMisses'] > 0 AS has_misses
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment LIKE '04068_probe_%'
ORDER BY event_time_microseconds, log_comment;

SELECT 'All range intersection and subcolumn tests passed';
