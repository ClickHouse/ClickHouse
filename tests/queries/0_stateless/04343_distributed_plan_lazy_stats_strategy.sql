-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Regression test for the lazy-statistics change. `estimateReadRowsCount` no longer builds a
-- statistics estimator by itself, so the distributed-plan strategy choices in makeDistributed.cpp
-- must build one (for the filter/prewhere columns on the path to the read step) and pass it in.
-- Otherwise a filtered relation takes the no-statistics fallback (which yields no row estimate once
-- a filter is present) and the distributed aggregation degrades to a shuffle.

SET enable_analyzer = 1;
SET materialize_statistics_on_insert = 1;
SET use_statistics_cache = 0;
SET enable_parallel_replicas = 0;
-- Distributed aggregation cannot enforce a global GROUP BY limit, so pin it to 0.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_dist_lazy_stats;

-- Wide-part table so each column's statistics is a separate blob: only the filter column's
-- statistics should be read to estimate the distributed-aggregation input row count.
CREATE TABLE t_dist_lazy_stats (k UInt64, a UInt64, b UInt64, c UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0,
         auto_statistics_types = 'uniq',
         refresh_statistics_interval = 0;

INSERT INTO t_dist_lazy_stats SELECT number % 10, number, number, number FROM numbers(1000);

-- INSERT pre-populates the per-part estimates cache; drop it so the queries below measure real loads.
DETACH TABLE t_dist_lazy_stats;
ATTACH TABLE t_dist_lazy_stats;

-- With statistics the estimator returns a bounded row estimate (<= distributed_plan_max_rows_to_broadcast),
-- so the planner keeps partial aggregation (ScatterExchange "any"). This is also the query whose
-- LoadedStatisticsColumns is measured below, so it must run first after the DETACH/ATTACH above.
SELECT 'aggregation strategy with statistics:',
       countIf(explain LIKE '%ScatterExchange (any)%') AS partial_aggregation
FROM (
  EXPLAIN SELECT k, count() FROM t_dist_lazy_stats WHERE a < 100 GROUP BY k
  SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, use_statistics = 1,
           use_statistics_for_part_pruning = 0, max_rows_to_group_by = 0
)
SETTINGS log_comment = '04343_dist_lazy_stats';

-- Without statistics the filtered read yields no estimate, so the planner falls back to a
-- shuffle aggregation (no ScatterExchange "any").
SELECT 'aggregation strategy without statistics:',
       countIf(explain LIKE '%ScatterExchange (any)%') AS partial_aggregation
FROM (
  EXPLAIN SELECT k, count() FROM t_dist_lazy_stats WHERE a < 100 GROUP BY k
  SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, use_statistics = 0,
           use_statistics_for_part_pruning = 0, max_rows_to_group_by = 0
);

SYSTEM FLUSH LOGS query_log;

-- Statistics are read for only the filter column `a` (1 column x 1 part), not for every column:
-- a regression that dropped the estimator would read 0, eager loading would read all 4 columns.
SELECT 'statistics columns loaded:', toUInt64(ProfileEvents['LoadedStatisticsColumns'])
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment = '04343_dist_lazy_stats'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_dist_lazy_stats;
