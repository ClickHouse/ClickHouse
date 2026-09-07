-- Tracked in https://github.com/ClickHouse/ClickHouse/issues/118534
-- `EXPLAIN ESTIMATE` of a query that `make_distributed_plan` distributes must still report the
-- parts, rows and marks of the `MergeTree` read. Today the estimate is empty: the `ReadFromMergeTree`
-- is hidden inside the distributed fragments (`ReadFromDistributedPlanSource`) and
-- `QueryPlan::explainEstimate` only walks the initiator plan.

DROP TABLE IF EXISTS t_05062;
CREATE TABLE t_05062 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_05062 SELECT number, number FROM numbers(1000);

SET enable_analyzer = 1, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0, max_rows_to_group_by = 0;

-- The query really distributes: strict mode would throw `SUPPORT_IS_DISABLED` otherwise.
SELECT k % 10 AS m, sum(v) FROM t_05062 GROUP BY m ORDER BY m LIMIT 1 SETTINGS distributed_plan_fallback_to_local_execution = 0;

SELECT 'estimate local', table, parts, rows, marks FROM (EXPLAIN ESTIMATE SELECT k % 10 AS m, sum(v) FROM t_05062 GROUP BY m ORDER BY m SETTINGS make_distributed_plan = 0);
SELECT 'estimate distributed', table, parts, rows, marks FROM (EXPLAIN ESTIMATE SELECT k % 10 AS m, sum(v) FROM t_05062 GROUP BY m ORDER BY m);
SELECT 'estimate strict', table, parts, rows, marks FROM (EXPLAIN ESTIMATE SELECT k % 10 AS m, sum(v) FROM t_05062 GROUP BY m ORDER BY m SETTINGS distributed_plan_fallback_to_local_execution = 0);
SELECT 'estimate cascades', table, parts, rows, marks FROM (EXPLAIN ESTIMATE SELECT k % 10 AS m, sum(v) FROM t_05062 GROUP BY m ORDER BY m SETTINGS enable_cascades_optimizer = 1, distributed_plan_workers_num = 2);
-- A query that falls back to local execution keeps its estimate.
SELECT 'estimate fallback', table, parts, rows, marks FROM (EXPLAIN ESTIMATE SELECT k % 10 AS m, sum(v) FROM t_05062 GROUP BY m WITH TOTALS ORDER BY m);

DROP TABLE t_05062;
