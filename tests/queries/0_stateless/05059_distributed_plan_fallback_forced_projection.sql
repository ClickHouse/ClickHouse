-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A distributed plan cannot guarantee that a projection is used, so a forced projection
-- (`force_optimize_projection` / `force_optimize_projection_name`) falls back to local execution:
-- the plan has no exchanges. Without forcing, the same query is distributed and has a GatherExchange.
-- `distributed_plan_max_rows_to_broadcast = 0` forces a bucketed read, which together with the
-- forced projection failed on the worker before the fallback.

SET enable_analyzer = 1, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_forced_projection;
CREATE TABLE t_forced_projection (k UInt32, v UInt64, PROJECTION p_agg (SELECT k, sum(v) GROUP BY k))
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_forced_projection SELECT number % 5, number FROM numbers(100000);

SELECT '-- force_optimize_projection falls back: no GatherExchange';
SELECT countIf(explain LIKE '%GatherExchange%') > 0
FROM (EXPLAIN SELECT k, sum(v) FROM t_forced_projection GROUP BY k ORDER BY k
      SETTINGS optimize_use_projections = 1, force_optimize_projection = 1);

SELECT '-- force_optimize_projection_name falls back: no GatherExchange';
SELECT countIf(explain LIKE '%GatherExchange%') > 0
FROM (EXPLAIN SELECT k, sum(v) FROM t_forced_projection GROUP BY k ORDER BY k
      SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'p_agg');

SELECT '-- without forcing, the query is distributed: GatherExchange present';
SELECT countIf(explain LIKE '%GatherExchange%') > 0
FROM (EXPLAIN SELECT k, sum(v) FROM t_forced_projection GROUP BY k ORDER BY k
      SETTINGS optimize_use_projections = 1);

DROP TABLE t_forced_projection;
