-- Checks the caching of part statistics during query planning:
-- the first query loads the statistics of every part from disk (part statistics cache misses),
-- the second query with the same part set reuses the cached selectivity estimator and does not
-- touch part statistics at all.
-- `refresh_statistics_interval = 0` disables the background refresh task so the cache
-- interactions below are fully deterministic, and the prewhere settings are pinned because the
-- estimator is built while moving conditions to `PREWHERE`.

DROP TABLE IF EXISTS t_part_stats_cache;

CREATE TABLE t_part_stats_cache (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS auto_statistics_types = 'basic, uniq_v2', refresh_statistics_interval = 0;

SYSTEM STOP MERGES t_part_stats_cache;

SET materialize_statistics_on_insert = 1;

INSERT INTO t_part_stats_cache SELECT number, number % 7 FROM numbers(1000);
INSERT INTO t_part_stats_cache SELECT number + 1000, number % 11 FROM numbers(1000);

SELECT count() FROM t_part_stats_cache WHERE a > 100 AND b > 1 SETTINGS use_statistics = 1, use_statistics_cache = 1, query_plan_optimize_prewhere = 1, optimize_move_to_prewhere = 1, log_comment = '05054_part_statistics_cache_first';
SELECT count() FROM t_part_stats_cache WHERE a > 100 AND b > 1 SETTINGS use_statistics = 1, use_statistics_cache = 1, query_plan_optimize_prewhere = 1, optimize_move_to_prewhere = 1, log_comment = '05054_part_statistics_cache_second';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['PartStatisticsCacheMisses'] >= 2
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05054_part_statistics_cache_first';

SELECT ProfileEvents['PartStatisticsCacheMisses'], ProfileEvents['SelectivityEstimatorCacheHits'] >= 1
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05054_part_statistics_cache_second';

DROP TABLE t_part_stats_cache;
