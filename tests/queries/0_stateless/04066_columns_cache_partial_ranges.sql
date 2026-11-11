-- Test columns cache with partial range reads
-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET max_threads = 1; -- Ensure deterministic read order for cache testing
SET use_columns_cache = 1;
SET enable_reads_from_columns_cache = 1;
SET enable_writes_to_columns_cache = 1;

DROP TABLE IF EXISTS t_cache_ranges;

-- Create table with specific granule configuration
CREATE TABLE t_cache_ranges (
    id UInt64,
    category String,
    value UInt64,
    data String
) ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000;

-- Insert 20000 rows (20 granules)
INSERT INTO t_cache_ranges
SELECT
    number AS id,
    'cat_' || toString(number % 50) AS category,
    number * 7 AS value,
    repeat('x', 50) AS data
FROM numbers(20000);

SYSTEM DROP COLUMNS CACHE;

-- Test 1: Read first granule
SELECT count(), sum(value) FROM t_cache_ranges WHERE id < 1000;
SELECT count(), sum(value) FROM t_cache_ranges WHERE id < 1000;

-- Test 2: Read different granules
SYSTEM DROP COLUMNS CACHE;
SELECT count() FROM t_cache_ranges WHERE id BETWEEN 0 AND 999;     -- Granule 0
SELECT count() FROM t_cache_ranges WHERE id BETWEEN 2000 AND 2999; -- Granule 2
SELECT count() FROM t_cache_ranges WHERE id BETWEEN 5000 AND 5999; -- Granule 5

-- Repeat to verify cache
SELECT count() FROM t_cache_ranges WHERE id BETWEEN 0 AND 999;
SELECT count() FROM t_cache_ranges WHERE id BETWEEN 2000 AND 2999;
SELECT count() FROM t_cache_ranges WHERE id BETWEEN 5000 AND 5999;

-- Test 3: Overlapping ranges
SYSTEM DROP COLUMNS CACHE;
SELECT sum(value) FROM t_cache_ranges WHERE id < 2000;  -- Granules 0-1
SELECT sum(value) FROM t_cache_ranges WHERE id < 1000;  -- Granule 0 (cached)

-- Test 4: Filtered reads
SYSTEM DROP COLUMNS CACHE;
SELECT count() FROM t_cache_ranges WHERE category = 'cat_10';
SELECT count() FROM t_cache_ranges WHERE category = 'cat_10';
SELECT count() FROM t_cache_ranges WHERE category = 'cat_20';
SELECT count() FROM t_cache_ranges WHERE category = 'cat_20';

-- Test 5: PREWHERE with cache
SELECT id, value FROM t_cache_ranges
PREWHERE category = 'cat_5'
WHERE value > 1000
ORDER BY id
LIMIT 10;

SELECT id, value FROM t_cache_ranges
PREWHERE category = 'cat_5'
WHERE value > 1000
ORDER BY id
LIMIT 10;

-- Test 6: Aggregations on ranges
SELECT category, count(*), sum(value)
FROM t_cache_ranges
WHERE id BETWEEN 5000 AND 10000
GROUP BY category
ORDER BY category
LIMIT 5;

SELECT category, count(*), sum(value)
FROM t_cache_ranges
WHERE id BETWEEN 5000 AND 10000
GROUP BY category
ORDER BY category
LIMIT 5;

-- Test 7: Multiple sequential reads
SYSTEM DROP COLUMNS CACHE;
SELECT sum(value) FROM t_cache_ranges WHERE id < 5000;
SELECT sum(value) FROM t_cache_ranges WHERE id < 5000;
SELECT sum(value) FROM t_cache_ranges WHERE id >= 5000 AND id < 10000;
SELECT sum(value) FROM t_cache_ranges WHERE id >= 5000 AND id < 10000;

-- Test 8: String column caching
SELECT count(DISTINCT data) FROM t_cache_ranges WHERE id < 10000;
SELECT count(DISTINCT data) FROM t_cache_ranges WHERE id < 10000;

DROP TABLE t_cache_ranges;

SELECT 'All partial range tests passed';
