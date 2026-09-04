-- Checks that statistics-based part pruning uses the part statistics cache regardless of the
-- query-level setting `use_statistics_cache`: the setting bypasses the caches only while the
-- selectivity estimator is built. Part pruning reads the statistics through the cache
-- unconditionally, so a pruning query that asks for the bypass still populates the cache, and
-- its estimates stay memoized on the parts for every later pruning query. This is the contract
-- described by the `refresh_statistics_interval` setting; a change here means that description
-- has to change too.
-- The selectivity estimator is not built for the pruning queries (`use_statistics = 0`), so part
-- pruning is their only consumer of the statistics. `refresh_statistics_interval = 0` disables the
-- background prewarm so the cache interactions below are fully deterministic. The table is
-- reattached once so that the first pruning query runs on fresh part objects: the estimates are
-- memoized on the part when it is written, and the parts written by the inserts would never
-- consult the cache. The two parts are in different partitions so they cannot be merged behind
-- the test's back (`SYSTEM STOP MERGES` does not survive the reattach).

DROP TABLE IF EXISTS t_stats_pruning_cache_bypass;

CREATE TABLE t_stats_pruning_cache_bypass (a UInt64, b UInt64) ENGINE = MergeTree PARTITION BY intDiv(a, 1000) ORDER BY a
SETTINGS auto_statistics_types = 'basic', refresh_statistics_interval = 0;

SET materialize_statistics_on_insert = 1;

INSERT INTO t_stats_pruning_cache_bypass SELECT number, number % 7 FROM numbers(1000);
INSERT INTO t_stats_pruning_cache_bypass SELECT number + 1000, number % 11 FROM numbers(1000);

DETACH TABLE t_stats_pruning_cache_bypass;
ATTACH TABLE t_stats_pruning_cache_bypass;

-- `b` never exceeds 10, so both parts are pruned by their statistics.
-- The cache is empty: the pruning query reads the statistics of every part from disk through
-- the cache (a miss per part), although it asks to bypass the cache.
SELECT count() FROM t_stats_pruning_cache_bypass WHERE b > 100 SETTINGS use_statistics = 0, use_statistics_cache = 0, use_statistics_for_part_pruning = 1, log_comment = '05061_pruning_cache_bypass_load';

-- The estimates memoized by the first pruning query serve the second one; the cache is not looked up at all.
SELECT count() FROM t_stats_pruning_cache_bypass WHERE b > 100 SETTINGS use_statistics = 0, use_statistics_cache = 0, use_statistics_for_part_pruning = 1, log_comment = '05061_pruning_cache_bypass_memoized';

-- The entries populated by the bypassing pruning query are the shared ones: building the
-- selectivity estimator finds the statistics of every part in the cache and reads nothing from
-- disk. Two conditions, so the estimator is built at all; the prewhere settings are pinned
-- because it is built while moving conditions to `PREWHERE`.
SELECT count() FROM t_stats_pruning_cache_bypass WHERE a > 100 AND b > 1 SETTINGS use_statistics = 1, use_statistics_cache = 1, use_statistics_for_part_pruning = 1, query_plan_optimize_prewhere = 1, optimize_move_to_prewhere = 1, log_comment = '05061_pruning_cache_bypass_estimator';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['SelectedParts'], ProfileEvents['PartStatisticsCacheMisses'] >= 2, ProfileEvents['PartStatisticsCacheHits']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05061_pruning_cache_bypass_load';

SELECT ProfileEvents['SelectedParts'], ProfileEvents['PartStatisticsCacheMisses'], ProfileEvents['PartStatisticsCacheHits']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05061_pruning_cache_bypass_memoized';

SELECT ProfileEvents['PartStatisticsCacheMisses'], ProfileEvents['PartStatisticsCacheHits'] >= 2
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05061_pruning_cache_bypass_estimator';

DROP TABLE t_stats_pruning_cache_bypass;
