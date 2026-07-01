SET enable_analyzer = 1;
SET materialize_statistics_on_insert = 1;
SET use_statistics_cache = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_win;
DROP TABLE IF EXISTS t_win_narrow;

-- Wide table: the filter column `a` and the join key `k` carry minmax statistics.
CREATE TABLE t_win (a UInt64, k UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0,
         auto_statistics_types = 'minmax',
         refresh_statistics_interval = 0;

CREATE TABLE t_win_narrow (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1073741824,
         auto_statistics_types = 'minmax',
         refresh_statistics_interval = 0;

INSERT INTO t_win SELECT number, number FROM numbers(1000);
INSERT INTO t_win SELECT number + 2000000, number FROM numbers(1000);
INSERT INTO t_win_narrow SELECT number, number % 10 FROM numbers(1000);

-- INSERT pre-populates the per-part estimates cache; drop it so the SELECTs below
-- measure real disk loads via `LoadedStatisticsColumns`.
DETACH TABLE t_win;
ATTACH TABLE t_win;
DETACH TABLE t_win_narrow;
ATTACH TABLE t_win_narrow;

-- Baseline: join with a plainly filtered relation (FilterStep -> ReadFromMergeTree).
SELECT count() FROM (SELECT a, k FROM t_win WHERE a > 1000000) AS w
INNER JOIN t_win_narrow AS n ON w.k = n.k WHERE n.v = 1
SETTINGS use_statistics_for_part_pruning = 0, use_statistics = 1,
         optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0,
         query_plan_optimize_join_order_limit = 10,
         log_comment = '04342_no_window'
FORMAT Null;

-- Same query, but a window function (a row-preserving transform) sits above the filter, so the
-- relation subtree is `WindowStep -> ... -> FilterStep -> ReadFromMergeTree`. Statistics must
-- still be discovered through the window step: `collectStatsColumnsForRelation` (column discovery)
-- and `estimateReadRowsCount` (the estimate fold) have to treat the generic row-preserving
-- transform the same way, otherwise no estimator is built and the relation loses its
-- statistics-backed estimate.
SELECT count() FROM (SELECT a, k, row_number() OVER (ORDER BY k) AS rn FROM t_win WHERE a > 1000000) AS w
INNER JOIN t_win_narrow AS n ON w.k = n.k WHERE n.v = 1
SETTINGS use_statistics_for_part_pruning = 0, use_statistics = 1,
         optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0,
         query_plan_optimize_join_order_limit = 10,
         log_comment = '04342_window'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- The window relation must load statistics (so the join estimate is statistics-backed), and it
-- must load exactly as many columns as the equivalent non-window relation -- the window step is
-- transparent to statistics collection. Before the fix the window query built no estimator for
-- the wide side and loaded fewer columns, so this would print 0.
WITH
    (
        SELECT toUInt64(ProfileEvents['LoadedStatisticsColumns'])
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND current_database = currentDatabase() AND type = 'QueryFinish'
          AND log_comment = '04342_window'
        ORDER BY event_time_microseconds DESC LIMIT 1
    ) AS window_loads,
    (
        SELECT toUInt64(ProfileEvents['LoadedStatisticsColumns'])
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND current_database = currentDatabase() AND type = 'QueryFinish'
          AND log_comment = '04342_no_window'
        ORDER BY event_time_microseconds DESC LIMIT 1
    ) AS no_window_loads
SELECT window_loads > 0 AND window_loads = no_window_loads;

DROP TABLE t_win;
DROP TABLE t_win_narrow;
