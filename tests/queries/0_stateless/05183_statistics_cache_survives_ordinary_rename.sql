-- Checks that renaming a table without a UUID (in an `Ordinary` database), which rewrites the
-- path of every live part and clears the caches of the server that are keyed by file path, keeps
-- the statistics caches usable: their entries are keyed by the table UUID (none for such a table)
-- and the part checksums, not by the table name or the part paths, so the entries warmed before
-- the rename are still found after it.
-- `refresh_statistics_interval = 0` disables the background refresh task so the cache
-- interactions below are fully deterministic, and the prewhere settings are pinned because the
-- estimator is built while moving conditions to `PREWHERE`.

-- The server warns about the deprecated database engine; that warning is not the subject here.
SET send_logs_level = 'fatal';
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;

-- A table without a UUID shares its cache entries with every other table whose parts have the
-- same contents, so another run of this test on the same server (concurrent, or an earlier one
-- that already warmed the caches) would turn the misses checked below into hits. The `run`
-- column, filled with the name of the run's database, makes the parts unique per run.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_stats_cache_rename (a UInt64, b UInt64, run String) ENGINE = MergeTree ORDER BY a
SETTINGS auto_statistics_types = 'basic, uniq_v2', refresh_statistics_interval = 0;

SYSTEM STOP MERGES {CLICKHOUSE_DATABASE_1:Identifier}.t_stats_cache_rename;

SET materialize_statistics_on_insert = 1;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_stats_cache_rename SELECT number, number % 7, {CLICKHOUSE_DATABASE:String} FROM numbers(1000);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_stats_cache_rename SELECT number + 1000, number % 11, {CLICKHOUSE_DATABASE:String} FROM numbers(1000);

-- Writing a part memoizes its estimates, so a pruning query would not touch the part statistics
-- cache; reload the parts so that the first pruning query populates the cache for every part.
DETACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_stats_cache_rename;
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_stats_cache_rename;
SYSTEM STOP MERGES {CLICKHOUSE_DATABASE_1:Identifier}.t_stats_cache_rename;

-- `b` never exceeds 10, so both parts are pruned by their statistics, loaded through the cache.
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_stats_cache_rename WHERE b > 100 SETTINGS use_statistics = 0, use_statistics_for_part_pruning = 1, log_comment = '05183_stats_cache_rename_warm_parts';

SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_stats_cache_rename WHERE a > 100 AND b > 1 SETTINGS use_statistics = 1, use_statistics_cache = 1, query_plan_optimize_prewhere = 1, optimize_move_to_prewhere = 1, log_comment = '05183_stats_cache_rename_warm_estimator';

RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_stats_cache_rename TO {CLICKHOUSE_DATABASE_1:Identifier}.t_stats_cache_renamed;

SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_stats_cache_renamed WHERE a > 100 AND b > 1 SETTINGS use_statistics = 1, use_statistics_cache = 1, query_plan_optimize_prewhere = 1, optimize_move_to_prewhere = 1, log_comment = '05183_stats_cache_rename_after_estimator';

-- A column set not seen before builds a new estimator from the part statistics.
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_stats_cache_renamed WHERE a > 100 AND a < 1900 SETTINGS use_statistics = 1, use_statistics_cache = 1, query_plan_optimize_prewhere = 1, optimize_move_to_prewhere = 1, log_comment = '05183_stats_cache_rename_after_parts';

SYSTEM FLUSH LOGS query_log;

-- The pruning query loads the statistics of every part from disk into the cache.
SELECT ProfileEvents['PartStatisticsCacheMisses'] >= 2
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05183_stats_cache_rename_warm_parts';

-- The estimator is built from the cached part statistics.
SELECT ProfileEvents['PartStatisticsCacheMisses'], ProfileEvents['PartStatisticsCacheHits'] >= 2
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05183_stats_cache_rename_warm_estimator';

-- The estimator cached before the rename is still served, so the statistics of the parts are not touched.
SELECT ProfileEvents['PartStatisticsCacheMisses'], ProfileEvents['SelectivityEstimatorCacheHits'] >= 1
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05183_stats_cache_rename_after_estimator';

-- The part statistics cached before the rename are still served to the new estimator.
SELECT ProfileEvents['PartStatisticsCacheMisses'], ProfileEvents['PartStatisticsCacheHits'] >= 2
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05183_stats_cache_rename_after_parts';

DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_stats_cache_renamed;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
