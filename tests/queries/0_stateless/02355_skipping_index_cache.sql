-- Tags: no-parallel
-- no-parallel: looks at server-wide metrics

-- Tests the skipping index cache.
SET parallel_replicas_local_plan=1;

SYSTEM DROP SKIPPING INDEX CACHE;
SELECT metric, value FROM system.metrics WHERE metric = 'SkippingIndexCacheSize';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id Int64, val Int64, INDEX idx val TYPE set(0) GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;
INSERT INTO tab SELECT number, number * 10 FROM numbers(1280);
SELECT metric, value FROM system.metrics WHERE metric = 'SkippingIndexCacheSize';

SELECT count() FROM tab where val = 25;
SELECT metric, value >= 1280 * 8, value < 1280 * 8 * 2 FROM system.metrics WHERE metric = 'SkippingIndexCacheSize';

SELECT count() FROM tab WHERE val = 30;
SELECT metric, value >= 1280 * 8, value < 1280 * 8 * 2 FROM system.metrics WHERE metric = 'SecondaryIndexCacheSize';

SYSTEM FLUSH LOGS;

SELECT query, ProfileEvents['SkippingIndexCacheHits'], ProfileEvents['SkippingIndexCacheMisses']
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT count()%'
ORDER BY event_time_microseconds;
