-- Test columns cache with partial granule reads and edge cases
-- Specifically tests the bug fix for granule counting with append reads
-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET max_threads = 1; -- Ensure deterministic read order for cache testing
SET use_columns_cache = 1;
SET enable_reads_from_columns_cache = 1;
SET enable_writes_to_columns_cache = 1;

DROP TABLE IF EXISTS t_cache_granules;

-- Create table with specific granule size for testing
CREATE TABLE t_cache_granules (
    id UInt64,
    category LowCardinality(String),
    value1 UInt64,
    value2 Float64,
    tags Array(String),
    data String
) ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    index_granularity = 8192;  -- Standard granule size

-- Insert exactly 50000 rows (6 full granules + 1 partial)
INSERT INTO t_cache_granules
SELECT
    number AS id,
    'cat_' || toString(number % 100) AS category,
    number * 7 AS value1,
    number * 1.5 AS value2,
    ['tag_' || toString(number % 10), 'tag_' || toString(number % 20)] AS tags,
    repeat('x', 100) AS data
FROM numbers(50000);

SYSTEM DROP COLUMNS CACHE;

-- Test 1: Read full granule (8192 rows)
SELECT count(*), sum(value1) FROM t_cache_granules WHERE id < 8192;
SELECT count(*), sum(value1) FROM t_cache_granules WHERE id < 8192;

-- Test 2: Read partial granule (first 1000 rows)
SYSTEM DROP COLUMNS CACHE;
SELECT count(*), sum(value1) FROM t_cache_granules WHERE id < 1000;
SELECT count(*), sum(value1) FROM t_cache_granules WHERE id < 1000;

-- Test 3: Read spanning two granules
SYSTEM DROP COLUMNS CACHE;
SELECT count(*), sum(value1) FROM t_cache_granules WHERE id BETWEEN 7000 AND 9000;
SELECT count(*), sum(value1) FROM t_cache_granules WHERE id BETWEEN 7000 AND 9000;

-- Test 4: Critical test for the bug fix - multiple reads with WHERE filter
-- This specifically tests the append case that was causing granule counting errors
SYSTEM DROP COLUMNS CACHE;

-- First read: populate cache
SELECT count(*) FROM t_cache_granules WHERE category = 'cat_42';

-- Second read: should use cache and handle append correctly (this was failing before fix)
SELECT count(*) FROM t_cache_granules WHERE category = 'cat_42';

-- Third read: verify stability
SELECT count(*) FROM t_cache_granules WHERE category = 'cat_42';

-- Test 5: Different filters on same column
SYSTEM DROP COLUMNS CACHE;

SELECT count(*) FROM t_cache_granules WHERE value1 > 100000;
SELECT count(*) FROM t_cache_granules WHERE value1 > 100000;
SELECT count(*) FROM t_cache_granules WHERE value1 > 200000;
SELECT count(*) FROM t_cache_granules WHERE value1 > 200000;

-- Test 6: Reading specific granule boundaries
SYSTEM DROP COLUMNS CACHE;

-- Granule 0: rows 0-8191
SELECT count(*) FROM t_cache_granules WHERE id < 8192;

-- Granule 1: rows 8192-16383
SELECT count(*) FROM t_cache_granules WHERE id BETWEEN 8192 AND 16383;

-- Granule 2: rows 16384-24575
SELECT count(*) FROM t_cache_granules WHERE id BETWEEN 16384 AND 24575;

-- Repeat to test cache
SELECT count(*) FROM t_cache_granules WHERE id < 8192;
SELECT count(*) FROM t_cache_granules WHERE id BETWEEN 8192 AND 16383;
SELECT count(*) FROM t_cache_granules WHERE id BETWEEN 16384 AND 24575;

-- Test 7: Partial reads within a single granule at different offsets
SYSTEM DROP COLUMNS CACHE;

SELECT count(*) FROM t_cache_granules WHERE id BETWEEN 1000 AND 2000;  -- Within granule 0
SELECT count(*) FROM t_cache_granules WHERE id BETWEEN 3000 AND 4000;  -- Within granule 0
SELECT count(*) FROM t_cache_granules WHERE id BETWEEN 5000 AND 6000;  -- Within granule 0

-- Repeat
SELECT count(*) FROM t_cache_granules WHERE id BETWEEN 1000 AND 2000;
SELECT count(*) FROM t_cache_granules WHERE id BETWEEN 3000 AND 4000;
SELECT count(*) FROM t_cache_granules WHERE id BETWEEN 5000 AND 6000;

-- Test 8: Read last partial granule (rows 49152-49999)
SELECT count(*) FROM t_cache_granules WHERE id >= 49152;
SELECT count(*) FROM t_cache_granules WHERE id >= 49152;

-- Test 9: Complex query with WHERE that triggers multiple granule reads
SYSTEM DROP COLUMNS CACHE;

SELECT category, count(*), sum(value1)
FROM t_cache_granules
WHERE id % 100 = 0
GROUP BY category
ORDER BY category
LIMIT 10;

SELECT category, count(*), sum(value1)
FROM t_cache_granules
WHERE id % 100 = 0
GROUP BY category
ORDER BY category
LIMIT 10;

-- Test 10: PREWHERE with partial granule reads
SELECT id, value1 FROM t_cache_granules
PREWHERE category = 'cat_10'
WHERE value2 > 100
LIMIT 100
FORMAT Hash;

SELECT id, value1 FROM t_cache_granules
PREWHERE category = 'cat_10'
WHERE value2 > 100
LIMIT 100
FORMAT Hash;

-- Test 11: Array column with partial reads
SELECT id, tags FROM t_cache_granules
WHERE id BETWEEN 10000 AND 11000
FORMAT Hash;

SELECT id, tags FROM t_cache_granules
WHERE id BETWEEN 10000 AND 11000
FORMAT Hash;

-- Test 12: Reading with LIMIT less than granule size
SELECT id, category, value1 FROM t_cache_granules
LIMIT 100
FORMAT Hash;

SELECT id, category, value1 FROM t_cache_granules
LIMIT 5000
FORMAT Hash;

SELECT id, category, value1 FROM t_cache_granules
LIMIT 100
FORMAT Hash;

-- Test 13: Reading with OFFSET
SELECT id, value1 FROM t_cache_granules
ORDER BY id
LIMIT 1000 OFFSET 8000
FORMAT Hash;  -- Crosses granule boundary

SELECT id, value1 FROM t_cache_granules
ORDER BY id
LIMIT 1000 OFFSET 8000
FORMAT Hash;

-- Test 14: Multiple different filters on cached data
SYSTEM DROP COLUMNS CACHE;

-- Populate cache with full column read
SELECT count(*) FROM t_cache_granules;

-- Now various filters (should all hit cache)
SELECT count(*) FROM t_cache_granules WHERE category = 'cat_5';
SELECT count(*) FROM t_cache_granules WHERE value1 > 50000;
SELECT count(*) FROM t_cache_granules WHERE id % 1000 = 0;

-- Repeat to verify cache hits
SELECT count(*) FROM t_cache_granules WHERE category = 'cat_5';
SELECT count(*) FROM t_cache_granules WHERE value1 > 50000;
SELECT count(*) FROM t_cache_granules WHERE id % 1000 = 0;

-- Test 15: Interleaved reads from different columns
SYSTEM DROP COLUMNS CACHE;

SELECT sum(value1) FROM t_cache_granules WHERE id < 10000;
SELECT sum(value2) FROM t_cache_granules WHERE id < 10000;
SELECT count(tags) FROM t_cache_granules WHERE id < 10000;

-- Repeat
SELECT sum(value1) FROM t_cache_granules WHERE id < 10000;
SELECT sum(value2) FROM t_cache_granules WHERE id < 10000;
SELECT count(tags) FROM t_cache_granules WHERE id < 10000;

-- Test 16: Regression test for the specific bug - alternating filters
SYSTEM DROP COLUMNS CACHE;

SELECT count(*) FROM t_cache_granules WHERE category <> '';
SELECT count(*) FROM t_cache_granules WHERE category <> '';
SELECT count(*) FROM t_cache_granules WHERE category <> '';

-- Test 17: Very small reads (single row)
SELECT * FROM t_cache_granules WHERE id = 12345 FORMAT Hash;
SELECT * FROM t_cache_granules WHERE id = 23456 FORMAT Hash;
SELECT * FROM t_cache_granules WHERE id = 34567 FORMAT Hash;

-- Repeat
SELECT * FROM t_cache_granules WHERE id = 12345 FORMAT Hash;
SELECT * FROM t_cache_granules WHERE id = 23456 FORMAT Hash;
SELECT * FROM t_cache_granules WHERE id = 34567 FORMAT Hash;

-- Test 18: Read all granules sequentially
SYSTEM DROP COLUMNS CACHE;

SELECT count(*) FROM t_cache_granules WHERE id < 8192;          -- Granule 0
SELECT count(*) FROM t_cache_granules WHERE id BETWEEN 8192 AND 16383;   -- Granule 1
SELECT count(*) FROM t_cache_granules WHERE id BETWEEN 16384 AND 24575;  -- Granule 2
SELECT count(*) FROM t_cache_granules WHERE id BETWEEN 24576 AND 32767;  -- Granule 3
SELECT count(*) FROM t_cache_granules WHERE id BETWEEN 32768 AND 40959;  -- Granule 4
SELECT count(*) FROM t_cache_granules WHERE id BETWEEN 40960 AND 49151;  -- Granule 5
SELECT count(*) FROM t_cache_granules WHERE id >= 49152;                 -- Granule 6 (partial)

-- Verify all cached
SELECT count(*) FROM t_cache_granules WHERE id < 8192;
SELECT count(*) FROM t_cache_granules WHERE id BETWEEN 8192 AND 16383;
SELECT count(*) FROM t_cache_granules WHERE id BETWEEN 16384 AND 24575;

-- Test 19: Aggregation with partial granule read
SELECT
    category,
    count(*) AS cnt,
    avg(value1) AS avg_val
FROM t_cache_granules
WHERE id BETWEEN 5000 AND 15000  -- Spans 2 granules
GROUP BY category
ORDER BY cnt DESC, category
LIMIT 5;

SELECT
    category,
    count(*) AS cnt,
    avg(value1) AS avg_val
FROM t_cache_granules
WHERE id BETWEEN 5000 AND 15000
GROUP BY category
ORDER BY cnt DESC, category
LIMIT 5;

-- Test 20: String column with partial reads
SELECT count(DISTINCT data) FROM t_cache_granules WHERE id < 10000;
SELECT count(DISTINCT data) FROM t_cache_granules WHERE id < 10000;

-- Test 21: Cache metrics verification (just check that at least one cache-related event exists)
SELECT count() > 0 AS has_cache_events
FROM system.events
WHERE event LIKE 'ColumnsCache%' AND value > 0
SETTINGS use_columns_cache = 0;

DROP TABLE t_cache_granules;

-- Create another table with very small granules to test edge cases
DROP TABLE IF EXISTS t_cache_small_granules;

CREATE TABLE t_cache_small_granules (
    id UInt64,
    value UInt64
) ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    index_granularity = 100;  -- Very small granules

INSERT INTO t_cache_small_granules SELECT number, number * 2 FROM numbers(1000);

SYSTEM DROP COLUMNS CACHE;

-- Test with many small granules
SELECT count(*), sum(value) FROM t_cache_small_granules WHERE id < 250;  -- 2.5 granules
SELECT count(*), sum(value) FROM t_cache_small_granules WHERE id BETWEEN 250 AND 500;  -- 2.5 granules
SELECT count(*), sum(value) FROM t_cache_small_granules WHERE id > 750;  -- 2.5 granules

-- Repeat to verify cache
SELECT count(*), sum(value) FROM t_cache_small_granules WHERE id < 250;
SELECT count(*), sum(value) FROM t_cache_small_granules WHERE id BETWEEN 250 AND 500;
SELECT count(*), sum(value) FROM t_cache_small_granules WHERE id > 750;

DROP TABLE t_cache_small_granules;

SELECT 'All partial granule read tests passed';
