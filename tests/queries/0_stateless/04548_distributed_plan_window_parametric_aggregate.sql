-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A parametric window aggregate must keep its parameters after WindowStep serialization. Regression
-- test for a bug where deserializeWindowFunctions rebuilt the aggregate from
-- aggregate_function->getParameters() instead of the parameters the planner preserved; parametric
-- aggregates such as groupArrayMovingSum do not round-trip their parameters that way, so the worker
-- silently rebuilt the no-parameter variant. groupArrayMovingSum(3) sums a sliding window of the last
-- 3 values; losing the parameter turns it into an unbounded moving sum (the reference below has the
-- capped-at-3 sums, e.g. the 5th element of the last row is 7+8+9 = 24, not a cumulative sum).

DROP TABLE IF EXISTS t_window_parametric;

CREATE TABLE t_window_parametric (id UInt32, v UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_window_parametric SELECT number, number FROM numbers(10);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0;

SELECT id, groupArrayMovingSum(3)(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s
FROM t_window_parametric ORDER BY id;

DROP TABLE t_window_parametric;
