-- Tags: no-parallel

DROP TABLE IF EXISTS t_primary_index_cache_2;

CREATE TABLE t_primary_index_cache_2 (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a PARTITION BY a % 2
SETTINGS use_primary_index_cache = 1, prewarm_primary_index_cache = 1, index_granularity = 64, index_granularity_bytes = '10M', min_bytes_for_wide_part = 0;

SYSTEM DROP PRIMARY INDEX CACHE;

INSERT INTO t_primary_index_cache_2 SELECT number, number FROM numbers(10000);

SYSTEM RELOAD ASYNCHRONOUS METRICS;
SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE table = 't_primary_index_cache_2' AND active;
SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN ('PrimaryIndexCacheFiles', 'PrimaryIndexCacheBytes') ORDER BY metric;

SELECT count() FROM t_primary_index_cache_2 WHERE a > 100 AND a < 200;

SYSTEM RELOAD ASYNCHRONOUS METRICS;
SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE table = 't_primary_index_cache_2' AND active;
SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN ('PrimaryIndexCacheFiles', 'PrimaryIndexCacheBytes') ORDER BY metric;

SYSTEM DROP PRIMARY INDEX CACHE;
SYSTEM PREWARM PRIMARY INDEX CACHE t_primary_index_cache_2;

SYSTEM RELOAD ASYNCHRONOUS METRICS;
SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE table = 't_primary_index_cache_2' AND active;
SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN ('PrimaryIndexCacheFiles', 'PrimaryIndexCacheBytes') ORDER BY metric;

SELECT count() FROM t_primary_index_cache_2 WHERE a > 100 AND a < 200 AND a % 2 = 0;

SYSTEM RELOAD ASYNCHRONOUS METRICS;
SELECT sum(primary_key_bytes_in_memory) FROM system.parts WHERE table = 't_primary_index_cache_2' AND active;
SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN ('PrimaryIndexCacheFiles', 'PrimaryIndexCacheBytes') ORDER BY metric;

SYSTEM FLUSH LOGS;

SELECT
    ProfileEvents['LoadedPrimaryIndexFiles'],
    ProfileEvents['LoadedPrimaryIndexRows'],
    ProfileEvents['LoadedPrimaryIndexBytes']
FROM system.query_log
WHERE query LIKE 'SELECT count() FROM t_primary_index_cache_2%' AND current_database = currentDatabase() AND type = 'QueryFinish'
ORDER BY event_time_microseconds;

DROP TABLE t_primary_index_cache_2;