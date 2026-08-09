-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- The plan header encoding does not carry the aggregate function state variant, so a Window-variant
-- state (CrossTab family) was decoded as an Aggregation-variant one and the deserialized plan header
-- was rejected. Results must match the non-distributed plan.

DROP TABLE IF EXISTS t_window_state_variant;

CREATE TABLE t_window_state_variant (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_window_state_variant SELECT number % 4, number % 3 FROM numbers(12);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0;

SELECT round(finalizeAggregation(theilsUState(a, b) OVER (ORDER BY a, b)), 6) AS s
FROM t_window_state_variant ORDER BY s;

SELECT round(finalizeAggregation(cramersVState(a, b) OVER (ORDER BY a, b)), 6) AS s
FROM t_window_state_variant ORDER BY s;

SELECT round(finalizeAggregation(contingencyState(a, b) OVER (ORDER BY a, b)), 6) AS s
FROM t_window_state_variant ORDER BY s;

-- Combinators propagate the variant to the wrapping function, and -ForEach nests the state in an Array.
SELECT round(finalizeAggregation(theilsUStateIf(a, b, a > 1) OVER (ORDER BY a, b)), 6) AS s
FROM t_window_state_variant ORDER BY s;

SELECT round(finalizeAggregation((theilsUStateForEach([a], [b]) OVER (ORDER BY a, b))[1]), 6) AS s
FROM t_window_state_variant ORDER BY s;

SELECT round(finalizeAggregation(theilsUArgMaxState(a, b, a) OVER (ORDER BY a, b)), 6) AS s
FROM t_window_state_variant ORDER BY s;

-- A step after the window step propagates the state column into its own header check.
SELECT round(finalizeAggregation(s), 6) AS f
FROM (SELECT theilsUState(a, b) OVER (ORDER BY a, b) AS s FROM t_window_state_variant)
ORDER BY f;

DROP TABLE t_window_state_variant;
