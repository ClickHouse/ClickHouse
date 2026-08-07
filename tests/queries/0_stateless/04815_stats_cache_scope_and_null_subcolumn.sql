-- Regression tests for the cached `ConditionSelectivityEstimator` reuse contract.

SET enable_analyzer = 1;
SET materialize_statistics_on_insert = 1;
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET use_statistics_cache = 1;
SET use_statistics_for_part_pruning = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;

-- Case 1: the cached estimator serves repeated requests for the same part set and
-- a subset of its columns, and is rebuilt (not wrongly reused or partially patched)
-- when the request needs a column it does not cover.

DROP TABLE IF EXISTS t_stats_scope;

CREATE TABLE t_stats_scope (p UInt8, b UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0,
         auto_statistics_types = 'basic',
         refresh_statistics_interval = 0;

INSERT INTO t_stats_scope SELECT 0, number FROM numbers(1000);
INSERT INTO t_stats_scope SELECT 1, number FROM numbers(1000);

-- INSERT pre-populates the per-part estimates cache; drop it so the SELECTs below
-- measure real disk loads via `LoadedStatisticsColumns`.
DETACH TABLE t_stats_scope;
ATTACH TABLE t_stats_scope;

-- Builds and caches the estimator for column `b` over both parts: 1 column x 2 parts = 2.
SELECT b FROM t_stats_scope WHERE b > 10 AND b < 500
SETTINGS log_comment = '04815_scope_full'
FORMAT Null;

-- Same columns, same part set: served from the cache, no loads.
SELECT b FROM t_stats_scope WHERE b > 10 AND b < 500
SETTINGS log_comment = '04815_scope_full_repeat'
FORMAT Null;

-- Adds a condition on `p`, which the cached estimator does not cover: rebuilt for
-- the requested columns, 2 columns x 2 parts = 4.
SELECT b FROM t_stats_scope WHERE p = 1 AND b > 10 AND b < 500
SETTINGS log_comment = '04815_scope_pruned'
FORMAT Null;

-- Case 2: with `optimize_functions_to_subcolumns` the planner requests the
-- virtual `<col>.null` subcolumn, while statistics are loaded and cached under
-- the physical parent column. A repeated `IS NULL` query must hit the cache
-- instead of rereading statistics from disk on every run.

DROP TABLE IF EXISTS t_stats_null;

CREATE TABLE t_stats_null (a Nullable(UInt64), b UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0,
         auto_statistics_types = 'basic',
         refresh_statistics_interval = 0;

INSERT INTO t_stats_null SELECT if(number % 10 = 0, NULL, number), number FROM numbers(1000);

DETACH TABLE t_stats_null;
ATTACH TABLE t_stats_null;

-- Loads statistics for `a` (requested as `a.null`) and `b`: 2 columns x 1 part = 2.
SELECT count() FROM t_stats_null WHERE a IS NULL AND b > 500
SETTINGS optimize_functions_to_subcolumns = 1, log_comment = '04815_null_first'
FORMAT Null;

-- The repeated query must be served from the cache: the `a.null` request is
-- satisfied by the cached statistics of the parent column `a`.
SELECT count() FROM t_stats_null WHERE a IS NULL AND b > 500
SETTINGS optimize_functions_to_subcolumns = 1, log_comment = '04815_null_repeat'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, toUInt64(ProfileEvents['LoadedStatisticsColumns'])
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment IN ('04815_scope_full', '04815_scope_full_repeat', '04815_scope_pruned', '04815_null_first', '04815_null_repeat')
ORDER BY log_comment, event_time_microseconds DESC
LIMIT 1 BY log_comment;

DROP TABLE t_stats_scope;
DROP TABLE t_stats_null;
