-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Window functions can be executed under make_distributed_plan=1: WindowStep is serialized for
-- remote execution and produces the same result as the non-distributed plan.

DROP TABLE IF EXISTS t_window_dist;

CREATE TABLE t_window_dist (a UInt32, v UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_window_dist SELECT number % 5, number FROM numbers(20);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0;

SELECT '-- sum OVER (PARTITION BY)';
SELECT a, v, sum(v) OVER (PARTITION BY a) AS s FROM t_window_dist ORDER BY a, v;

SELECT '-- row_number with ORDER BY';
SELECT a, v, row_number() OVER (PARTITION BY a ORDER BY v) AS rn FROM t_window_dist ORDER BY a, v;

SELECT '-- rolling frame (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)';
SELECT a, v, sum(v) OVER (PARTITION BY a ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS roll
FROM t_window_dist ORDER BY a, v;

DROP TABLE t_window_dist;
