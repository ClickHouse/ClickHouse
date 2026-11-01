-- Test columns cache with JSON column type
-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET max_threads = 1; -- Ensure deterministic read order for cache testing
SET use_columns_cache = 1;
SET enable_reads_from_columns_cache = 1;
SET enable_writes_to_columns_cache = 1;

DROP TABLE IF EXISTS t_cache_json;

-- Create table with JSON column
CREATE TABLE t_cache_json (
    id UInt64,
    data JSON,
    timestamp DateTime
) ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 500;

-- Insert test data with various JSON structures
INSERT INTO t_cache_json
SELECT
    number AS id,
    '{"name": "user_' || toString(number) || '", "age": ' || toString(20 + (number % 50)) || ', "city": "city_' || toString(number % 20) || '", "tags": ["tag' || toString(number % 5) || '", "tag' || toString(number % 7) || '"], "metrics": {"views": ' || toString(number * 10) || ', "clicks": ' || toString(number * 5) || '}}' AS data,
    now() - INTERVAL number SECOND AS timestamp
FROM numbers(5000);

SYSTEM DROP COLUMNS CACHE;

-- =============================================================================
-- Test 1: Basic JSON column caching
-- =============================================================================

SELECT 'Test 1: Basic JSON column read and cache';

-- First read: populate cache
SELECT count(), sum(CAST(data.age AS UInt64)) FROM t_cache_json WHERE id < 1000;

-- Second read: from cache
SELECT count(), sum(CAST(data.age AS UInt64)) FROM t_cache_json WHERE id < 1000;

-- Third read: verify cached
SELECT count(), sum(CAST(data.age AS UInt64)) FROM t_cache_json WHERE id < 1000;

-- =============================================================================
-- Test 2: JSON nested field access
-- =============================================================================

SELECT 'Test 2: JSON nested field access';

SYSTEM DROP COLUMNS CACHE;

-- Access nested metrics
SELECT count(), sum(CAST(data.metrics.views AS UInt64)), sum(CAST(data.metrics.clicks AS UInt64)) FROM t_cache_json WHERE id >= 1000 AND id < 2000;

-- Cached read
SELECT count(), sum(CAST(data.metrics.views AS UInt64)), sum(CAST(data.metrics.clicks AS UInt64)) FROM t_cache_json WHERE id >= 1000 AND id < 2000;

-- =============================================================================
-- Test 3: JSON with partial ranges
-- =============================================================================

SELECT 'Test 3: JSON with partial range reads';

SYSTEM DROP COLUMNS CACHE;

-- Read range [2000, 4000) - marks 4-8
SELECT count(), min(CAST(data.name AS String)) FROM t_cache_json WHERE id >= 2000 AND id < 4000;

-- Read subset [2500, 3500) - should use cache for marks 5-7
SELECT count(), min(CAST(data.city AS String)) FROM t_cache_json WHERE id >= 2500 AND id < 3500;

-- Verify both ranges cached
SELECT count(), min(CAST(data.name AS String)) FROM t_cache_json WHERE id >= 2000 AND id < 4000;
SELECT count(), min(CAST(data.city AS String)) FROM t_cache_json WHERE id >= 2500 AND id < 3500;

-- =============================================================================
-- Test 4: JSON string field access and caching
-- =============================================================================

SELECT 'Test 4: JSON string field access';

SYSTEM DROP COLUMNS CACHE;

-- Access JSON string field
SELECT count(), min(CAST(data.name AS String)) FROM t_cache_json WHERE id < 2000;

-- Cached read
SELECT count(), min(CAST(data.name AS String)) FROM t_cache_json WHERE id < 2000;

-- =============================================================================
-- Test 5: Complex JSON queries with cache
-- =============================================================================

SELECT 'Test 5: Complex JSON queries';

SYSTEM DROP COLUMNS CACHE;

-- Complex query with multiple JSON field accesses
SELECT
    CAST(data.city AS String) AS city,
    count() AS cnt,
    avg(CAST(data.age AS UInt64)) AS avg_age,
    sum(CAST(data.metrics.views AS UInt64)) AS total_views
FROM t_cache_json
WHERE id < 3000
GROUP BY city
ORDER BY cnt DESC, city
LIMIT 5;

-- Same query from cache
SELECT
    CAST(data.city AS String) AS city,
    count() AS cnt,
    avg(CAST(data.age AS UInt64)) AS avg_age,
    sum(CAST(data.metrics.views AS UInt64)) AS total_views
FROM t_cache_json
WHERE id < 3000
GROUP BY city
ORDER BY cnt DESC, city
LIMIT 5;

-- =============================================================================
-- Test 6: JSON with PREWHERE
-- =============================================================================

SELECT 'Test 6: JSON with PREWHERE';

SYSTEM DROP COLUMNS CACHE;

-- PREWHERE on id, WHERE on JSON field
SELECT count(), sum(CAST(data.metrics.clicks AS UInt64))
FROM t_cache_json
PREWHERE id >= 3000 AND id < 4000
WHERE CAST(data.age AS UInt64) > 30;

-- Cached read
SELECT count(), sum(CAST(data.metrics.clicks AS UInt64))
FROM t_cache_json
PREWHERE id >= 3000 AND id < 4000
WHERE CAST(data.age AS UInt64) > 30;

-- =============================================================================
-- Test 7: JSON column with intersecting ranges
-- =============================================================================

SELECT 'Test 7: JSON with range intersections';

SYSTEM DROP COLUMNS CACHE;

-- First range [1000, 3000)
SELECT count(), avg(CAST(data.age AS UInt64)) FROM t_cache_json WHERE id >= 1000 AND id < 3000;

-- Overlapping range [2000, 4000)
SELECT count(), avg(CAST(data.metrics.views AS UInt64)) FROM t_cache_json WHERE id >= 2000 AND id < 4000;

-- Verify both work from cache
SELECT count(), avg(CAST(data.age AS UInt64)) FROM t_cache_json WHERE id >= 1000 AND id < 3000;
SELECT count(), avg(CAST(data.metrics.views AS UInt64)) FROM t_cache_json WHERE id >= 2000 AND id < 4000;

-- =============================================================================
-- Test 8: JSON correctness verification
-- =============================================================================

SELECT 'Test 8: Verify JSON data correctness';

SYSTEM DROP COLUMNS CACHE;

-- Compute results without cache
SET use_columns_cache = 0;
SELECT sum(CAST(data.age AS UInt64)), sum(CAST(data.metrics.views AS UInt64)), sum(CAST(data.metrics.clicks AS UInt64)) FROM t_cache_json WHERE id BETWEEN 1500 AND 3500;

-- Now with cache
SET use_columns_cache = 1;
SYSTEM DROP COLUMNS CACHE;
SELECT sum(CAST(data.age AS UInt64)), sum(CAST(data.metrics.views AS UInt64)), sum(CAST(data.metrics.clicks AS UInt64)) FROM t_cache_json WHERE id BETWEEN 1500 AND 3500;

-- Cached read should match
SELECT sum(CAST(data.age AS UInt64)), sum(CAST(data.metrics.views AS UInt64)), sum(CAST(data.metrics.clicks AS UInt64)) FROM t_cache_json WHERE id BETWEEN 1500 AND 3500;

DROP TABLE t_cache_json;

SELECT 'All JSON caching tests passed';
