-- Tags: no-parallel
-- Tag no-parallel: `SYSTEM DROP STATISTICS CACHE` clears the server-wide statistics caches.

-- Checks that `SYSTEM DROP STATISTICS CACHE` also drops the estimates memoized on the data parts
-- for statistics-based part pruning: once memoized (when the part is written, or by the first
-- pruning query), the estimates serve every later pruning query without touching the part
-- statistics cache at all, and after the drop the statistics are read from disk again.
-- The selectivity estimator is not built (`use_statistics = 0`), so part pruning is the only
-- consumer of the statistics here. `refresh_statistics_interval = 0` disables the background
-- prewarm so the cache interactions below are fully deterministic.

DROP TABLE IF EXISTS t_stats_cache_drop_pruning;

CREATE TABLE t_stats_cache_drop_pruning (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS auto_statistics_types = 'basic', refresh_statistics_interval = 0;

SYSTEM STOP MERGES t_stats_cache_drop_pruning;

SET materialize_statistics_on_insert = 1;

INSERT INTO t_stats_cache_drop_pruning SELECT number, number % 7 FROM numbers(1000);
INSERT INTO t_stats_cache_drop_pruning SELECT number + 1000, number % 11 FROM numbers(1000);

-- `b` never exceeds 10, so both parts are pruned by their statistics.
SELECT count() FROM t_stats_cache_drop_pruning WHERE b > 100 SETTINGS use_statistics = 0, use_statistics_for_part_pruning = 1, log_comment = '05060_part_estimates_first';
SELECT count() FROM t_stats_cache_drop_pruning WHERE b > 100 SETTINGS use_statistics = 0, use_statistics_for_part_pruning = 1, log_comment = '05060_part_estimates_memoized';

SYSTEM DROP STATISTICS CACHE;

SELECT count() FROM t_stats_cache_drop_pruning WHERE b > 100 SETTINGS use_statistics = 0, use_statistics_for_part_pruning = 1, log_comment = '05060_part_estimates_after_drop';

SYSTEM FLUSH LOGS query_log;

-- Every part is pruned by its statistics.
SELECT ProfileEvents['SelectedParts']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05060_part_estimates_first';

-- The memoized estimates serve the pruning; the part statistics cache is not looked up at all.
SELECT ProfileEvents['SelectedParts'], ProfileEvents['PartStatisticsCacheMisses'], ProfileEvents['PartStatisticsCacheHits']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05060_part_estimates_memoized';

-- After the drop, the memoized estimates are gone and the statistics of every part are read from
-- disk again (a surviving part statistics cache entry would be a hit instead).
SELECT ProfileEvents['SelectedParts'], ProfileEvents['PartStatisticsCacheMisses'] >= 2, ProfileEvents['PartStatisticsCacheHits']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05060_part_estimates_after_drop';

DROP TABLE t_stats_cache_drop_pruning;
