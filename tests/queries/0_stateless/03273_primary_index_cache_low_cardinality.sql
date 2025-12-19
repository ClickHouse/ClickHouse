-- Tags: no-parallel

DROP TABLE IF EXISTS t_primary_index_cache;

SYSTEM DROP PRIMARY INDEX CACHE;

CREATE TABLE t_primary_index_cache (a LowCardinality(String), b LowCardinality(String))
ENGINE = MergeTree ORDER BY (a, b)
SETTINGS use_primary_key_cache = 1, prewarm_primary_key_cache = 1, index_granularity = 8192, index_granularity_bytes = '10M', min_bytes_for_wide_part = 0;

-- Insert will prewarm primary index cache
INSERT INTO t_primary_index_cache SELECT number%10, number%11 FROM numbers(10000);

-- Check cache size
SYSTEM RELOAD ASYNCHRONOUS METRICS;
SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN ('PrimaryIndexCacheFiles', 'PrimaryIndexCacheBytes') ORDER BY metric;

SYSTEM DROP PRIMARY INDEX CACHE;

-- Check that cache is empty
SYSTEM RELOAD ASYNCHRONOUS METRICS;
SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN ('PrimaryIndexCacheFiles', 'PrimaryIndexCacheBytes') ORDER BY metric;

-- Trigger index reload
SELECT max(length(a || b)) FROM t_primary_index_cache WHERE a > '1' AND b < '99' SETTINGS log_comment = '03273_reload_query';

-- Check that cache size is the same as after prewarm
SYSTEM RELOAD ASYNCHRONOUS METRICS;
SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN ('PrimaryIndexCacheFiles', 'PrimaryIndexCacheBytes') ORDER BY metric;

SYSTEM FLUSH LOGS;

SELECT
    ProfileEvents['LoadedPrimaryIndexFiles'],
    ProfileEvents['LoadedPrimaryIndexRows'],
    ProfileEvents['LoadedPrimaryIndexBytes']
FROM system.query_log
WHERE log_comment = '03273_reload_query' AND current_database = currentDatabase() AND type = 'QueryFinish'
ORDER BY event_time_microseconds;

DROP TABLE t_primary_index_cache;
