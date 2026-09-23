-- Tags: no-parallel-replicas, no-parallel
-- no-parallel: the oracle reads the process-global mark cache, which another test's
-- SYSTEM CLEAR MARK CACHE would empty; re-warming instead is impossible because
-- SYSTEM PREWARM MARK CACHE does not recurse into projections, and would anyway insert
-- under the read key and make the test pass with only half the fix.
-- no-parallel-replicas: a projection cannot serve the query when parallel replicas are
-- enabled without a local plan or with aggregation in order.

DROP TABLE IF EXISTS t_mc_proj;

CREATE TABLE t_mc_proj (k UInt64, a UInt64, b String, c String, d String, PROJECTION p (SELECT k, sum(a) GROUP BY k))
ENGINE = MergeTree ORDER BY k
SETTINGS prewarm_mark_cache = 1, min_bytes_to_prewarm_caches = 0, min_bytes_for_wide_part = '1G', index_granularity = 8192;

INSERT INTO t_mc_proj SELECT number % 100, number, 'x', 'y', 'z' FROM numbers(100000);

SELECT k, sum(a) FROM t_mc_proj GROUP BY k
SETTINGS log_comment = '05243_projection_read', optimize_use_projections = 1
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    has(projections, currentDatabase() || '.t_mc_proj.p') AS served_by_projection,
    ProfileEvents['MarkCacheMisses'] = 0 AS no_mark_cache_misses,
    ProfileEvents['MarkCacheHits'] > 0 AS marks_found_in_cache
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
  AND log_comment = '05243_projection_read' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_mc_proj;
