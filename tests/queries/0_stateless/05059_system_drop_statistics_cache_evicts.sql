-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: `SYSTEM DROP STATISTICS CACHE` clears the server-wide statistics caches.

-- Checks that `SYSTEM DROP STATISTICS CACHE` evicts both the part statistics cache and the
-- selectivity estimator cache: after the drop, the query plans from scratch again, so the
-- statistics of every part are read from disk and the estimator is rebuilt.
-- `refresh_statistics_interval = 0` disables the background refresh task so the cache
-- interactions below are fully deterministic, and the prewhere settings are pinned because the
-- estimator is built while moving conditions to `PREWHERE`.

DROP TABLE IF EXISTS t_stats_cache_drop;

CREATE TABLE t_stats_cache_drop (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS auto_statistics_types = 'basic, uniq_v2', refresh_statistics_interval = 0;

SYSTEM STOP MERGES t_stats_cache_drop;

SET materialize_statistics_on_insert = 1;

INSERT INTO t_stats_cache_drop SELECT number, number % 7 FROM numbers(1000);
INSERT INTO t_stats_cache_drop SELECT number + 1000, number % 11 FROM numbers(1000);

SELECT count() FROM t_stats_cache_drop WHERE a > 100 AND b > 1 SETTINGS use_statistics = 1, use_statistics_cache = 1, query_plan_optimize_prewhere = 1, optimize_move_to_prewhere = 1, log_comment = '05059_stats_cache_drop_warm';
SELECT count() FROM t_stats_cache_drop WHERE a > 100 AND b > 1 SETTINGS use_statistics = 1, use_statistics_cache = 1, query_plan_optimize_prewhere = 1, optimize_move_to_prewhere = 1, log_comment = '05059_stats_cache_drop_cached';

SYSTEM DROP STATISTICS CACHE;

SELECT count() FROM t_stats_cache_drop WHERE a > 100 AND b > 1 SETTINGS use_statistics = 1, use_statistics_cache = 1, query_plan_optimize_prewhere = 1, optimize_move_to_prewhere = 1, log_comment = '05059_stats_cache_drop_after_drop';

SYSTEM FLUSH LOGS query_log;

-- The estimator is cached before the drop, so the statistics of the parts are not touched again.
SELECT ProfileEvents['PartStatisticsCacheMisses'], ProfileEvents['SelectivityEstimatorCacheHits'] >= 1
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05059_stats_cache_drop_cached';

-- After the drop, the estimator cache misses (so the estimator cache was cleared) and the
-- statistics of every part are read from disk again (so the part statistics cache was cleared,
-- too: a surviving entry would be a hit instead).
SELECT ProfileEvents['PartStatisticsCacheMisses'] >= 2, ProfileEvents['SelectivityEstimatorCacheHits']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05059_stats_cache_drop_after_drop';

DROP TABLE t_stats_cache_drop;
