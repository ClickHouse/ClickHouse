-- Regression test: WITH TOTALS / ROLLUP / CUBE produce extra streams (a totals stream, or subtotal
-- rows from a Rollup/Cube step) that the distributed exchange protocol does not carry. Such plans
-- must not be distributed; the distributed-plan result must match the single-node result.
-- (Without the guard, WITH TOTALS loses/corrupts the totals stream and ROLLUP/CUBE hang.)

DROP TABLE IF EXISTS t_totals_guard;

CREATE TABLE t_totals_guard (a UInt32, v UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_totals_guard SELECT number % 10, number FROM numbers(100000);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, enable_join_runtime_filters = 0,
    distributed_plan_force_shuffle_aggregation = 1;

SELECT '-- WITH TOTALS';
SELECT a, sum(v) FROM t_totals_guard GROUP BY a WITH TOTALS ORDER BY a;

SELECT '-- ROLLUP';
SELECT a, sum(v) FROM t_totals_guard GROUP BY a WITH ROLLUP ORDER BY a;

SELECT '-- CUBE';
SELECT a, sum(v) FROM t_totals_guard GROUP BY a WITH CUBE ORDER BY a;

DROP TABLE t_totals_guard;
