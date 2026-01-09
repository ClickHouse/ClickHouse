-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Test partial aggregate cache for GROUP BY queries on MergeTree tables

DROP TABLE IF EXISTS test_partial_agg_cache;

-- Create table and insert data into multiple parts
CREATE TABLE test_partial_agg_cache (
    date Date,
    category String,
    value Int64
) ENGINE = MergeTree()
PARTITION BY date
ORDER BY (date, category);

-- Insert data for different dates (creates multiple parts)
INSERT INTO test_partial_agg_cache SELECT '2024-01-01', 'A', number FROM numbers(10000);
INSERT INTO test_partial_agg_cache SELECT '2024-01-01', 'B', number FROM numbers(10000);
INSERT INTO test_partial_agg_cache SELECT '2024-01-02', 'A', number FROM numbers(10000);
INSERT INTO test_partial_agg_cache SELECT '2024-01-02', 'B', number FROM numbers(10000);

SELECT '--- First query (cache miss expected)';

-- First aggregation query - should miss cache
SELECT category, sum(value), count()
FROM test_partial_agg_cache
GROUP BY category
ORDER BY category
SETTINGS use_partial_aggregate_cache = 1;

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query LIKE '%FROM test_partial_agg_cache%GROUP BY category%use_partial_aggregate_cache = 1%'
    AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '--- Second query (cache hit expected)';

-- Same query again - should hit cache
SELECT category, sum(value), count()
FROM test_partial_agg_cache
GROUP BY category
ORDER BY category
SETTINGS use_partial_aggregate_cache = 1;

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query LIKE '%FROM test_partial_agg_cache%GROUP BY category%use_partial_aggregate_cache = 1%'
    AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '--- Insert new data and query again';

-- Insert new data (new part)
INSERT INTO test_partial_agg_cache SELECT '2024-01-03', 'A', number FROM numbers(10000);
INSERT INTO test_partial_agg_cache SELECT '2024-01-03', 'B', number FROM numbers(10000);

-- Query again - should have mix of hits (old parts) and misses (new part)
SELECT category, sum(value), count()
FROM test_partial_agg_cache
GROUP BY category
ORDER BY category
SETTINGS use_partial_aggregate_cache = 1;

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query LIKE '%FROM test_partial_agg_cache%GROUP BY category%use_partial_aggregate_cache = 1%'
    AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE test_partial_agg_cache;

