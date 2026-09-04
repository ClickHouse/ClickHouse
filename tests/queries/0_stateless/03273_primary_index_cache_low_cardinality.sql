-- Tags: no-parallel
-- Tag no-parallel: uses shared cache state and must remain isolated from concurrent cache tests.
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS t_primary_index_cache;


CREATE TABLE t_primary_index_cache (a LowCardinality(String), b LowCardinality(String))
ENGINE = MergeTree ORDER BY (a, b)
SETTINGS use_primary_key_cache = 1, prewarm_primary_key_cache = 1, index_granularity = 8192, index_granularity_bytes = '10M', min_bytes_for_wide_part = 0;

-- Insert will prewarm primary index cache
INSERT INTO t_primary_index_cache SELECT number%10, number%11 FROM numbers(10000);

SYSTEM CLEAR PRIMARY INDEX CACHE;

-- Trigger index reload
SELECT max(length(a || b)) FROM t_primary_index_cache WHERE a > '1' AND b < '99' SETTINGS log_comment = '03273_reload_query';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['LoadedPrimaryIndexFiles'],
    ProfileEvents['LoadedPrimaryIndexRows'],
    ProfileEvents['LoadedPrimaryIndexBytes']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND log_comment = '03273_reload_query' AND current_database = currentDatabase() AND type = 'QueryFinish'
ORDER BY event_time_microseconds;

DROP TABLE t_primary_index_cache;
