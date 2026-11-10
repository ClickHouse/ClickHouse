-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Test WindowStep serialization of a frame whose END boundary is PRECEDING (frame.end_preceding=true)
-- under make_distributed_plan. Result must match the non-distributed plan.

DROP TABLE IF EXISTS t_window_end_preceding;

CREATE TABLE t_window_end_preceding (v UInt32) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_window_end_preceding SELECT number FROM numbers(20);

SELECT v, sum(v) OVER (ORDER BY v ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS s
FROM t_window_end_preceding
ORDER BY v
SETTINGS make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0;

DROP TABLE t_window_end_preceding;
