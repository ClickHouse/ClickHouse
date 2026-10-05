-- Checks that dropping a table without a UUID (in an `Ordinary` database), which clears the
-- caches of the server that are keyed by file path, keeps the statistics caches: their entries
-- are keyed by the part checksums, so a table recreated at the same path cannot collide with them.
-- `refresh_statistics_interval = 0` disables the background refresh task so the cache
-- interactions below are fully deterministic, and the prewhere settings are pinned because the
-- estimator is built while moving conditions to `PREWHERE`.

DROP TABLE IF EXISTS t_stats_cache_ordinary_drop;

CREATE TABLE t_stats_cache_ordinary_drop (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS auto_statistics_types = 'basic, uniq_v2', refresh_statistics_interval = 0;

SYSTEM STOP MERGES t_stats_cache_ordinary_drop;

SET materialize_statistics_on_insert = 1;

INSERT INTO t_stats_cache_ordinary_drop SELECT number, number % 7 FROM numbers(1000);
INSERT INTO t_stats_cache_ordinary_drop SELECT number + 1000, number % 11 FROM numbers(1000);

SELECT count() FROM t_stats_cache_ordinary_drop WHERE a > 100 AND b > 1 SETTINGS use_statistics = 1, use_statistics_cache = 1, query_plan_optimize_prewhere = 1, optimize_move_to_prewhere = 1, log_comment = '05137_stats_cache_ordinary_drop_warm';

-- The server warns about the deprecated database engine; that warning is not the subject here.
SET send_logs_level = 'fatal';
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_no_uuid (x UInt32) ENGINE = MergeTree ORDER BY x;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_no_uuid;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT count() FROM t_stats_cache_ordinary_drop WHERE a > 100 AND b > 1 SETTINGS use_statistics = 1, use_statistics_cache = 1, query_plan_optimize_prewhere = 1, optimize_move_to_prewhere = 1, log_comment = '05137_stats_cache_ordinary_drop_after';

SYSTEM FLUSH LOGS query_log;

-- The estimator cached before the drop is still served, so the statistics of the parts are not touched.
SELECT ProfileEvents['PartStatisticsCacheMisses'], ProfileEvents['SelectivityEstimatorCacheHits'] >= 1
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05137_stats_cache_ordinary_drop_after';

DROP TABLE t_stats_cache_ordinary_drop;
