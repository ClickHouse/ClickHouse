-- Tags: no-parallel, no-random-settings
-- Tag no-parallel: toggles a server-global failpoint.
-- Tag no-random-settings: keeps the statistics paths and settings deterministic.

-- Building the selectivity estimator must read the statistics of the referenced columns only,
-- whether or not the part statistics cache is used: a query on `a` must not open the statistics
-- file of `b`, so an unreadable one cannot fail it.

SET allow_statistics = 1;
SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

SYSTEM DISABLE FAILPOINT merge_tree_load_statistics_throw;
DROP TABLE IF EXISTS t_narrow_stats_load;

-- `auto_statistics_types` is emptied so that `b` is the only column with statistics, and
-- `refresh_statistics_interval = 0` disables the background prewarm, which loads all columns.
CREATE TABLE t_narrow_stats_load (a UInt64, b UInt64 STATISTICS(basic))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = '', refresh_statistics_interval = 0;

INSERT INTO t_narrow_stats_load SELECT number, number FROM numbers(1000);

SYSTEM ENABLE FAILPOINT merge_tree_load_statistics_throw;

-- Two conditions, so the estimator is built at all. Part pruning is disabled because it estimates
-- every column of the part by design and is unrelated to the column set asked for here.
SELECT count() FROM t_narrow_stats_load WHERE a > 500 AND a < 900
SETTINGS use_statistics = 1, use_statistics_cache = 1, use_statistics_for_part_pruning = 0,
         query_plan_optimize_prewhere = 1, optimize_move_to_prewhere = 1;

SELECT count() FROM t_narrow_stats_load WHERE a > 500 AND a < 900
SETTINGS use_statistics = 1, use_statistics_cache = 0, use_statistics_for_part_pruning = 0,
         query_plan_optimize_prewhere = 1, optimize_move_to_prewhere = 1;

-- The referenced column is the one with statistics, so its unreadable file does fail the query.
SELECT count() FROM t_narrow_stats_load WHERE b > 500 AND b < 900
SETTINGS use_statistics = 1, use_statistics_cache = 1, use_statistics_for_part_pruning = 0,
         query_plan_optimize_prewhere = 1, optimize_move_to_prewhere = 1; -- { serverError CANNOT_READ_ALL_DATA }

SYSTEM DISABLE FAILPOINT merge_tree_load_statistics_throw;

DROP TABLE t_narrow_stats_load;
