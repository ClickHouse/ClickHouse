-- Tags: no-old-analyzer
-- no-old-analyzer: distributed planning requires the analyzer.

-- A read is priced on the rows the primary key keeps, not on the rows the filter keeps.
-- `t_rc_dim` is filtered on `v`, which is not in the sorting key, so the scan covers the whole
-- table although the estimate (from column statistics) is a few hundred rows. Without the
-- scan-volume cost the optimizer replicates the read and every node re-scans the whole table;
-- with it the table is scanned once in parallel and the small result is broadcast.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 8;
-- The tables are small, so remove the fixed exchange cost: it would decide the plan instead
-- of the read and transfer volumes the test is about.
SET param__internal_cascades_cost_config = '{"exchange_fixed_overhead":1}';
-- The decision needs the statistics-based filter estimate; pin everything it depends on.
SET allow_statistic_optimize = 1;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
-- Pinned (randomized in CI): join-order jitter changes the shape around the asserted reads,
-- and the `PREWHERE` move flips the `WHERE` step between `Filter` and `Expression`.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_algorithm = 'dpsize greedy';
SET optimize_move_to_prewhere = 1;
SET query_plan_merge_filters = 1;
SET query_plan_optimize_prewhere = 1;
SET enable_multiple_prewhere_read_steps = 1;
SET allow_reorder_prewhere_conditions = 1;

DROP TABLE IF EXISTS t_rc_probe;
DROP TABLE IF EXISTS t_rc_dim;
-- The wide probe column makes shuffling the probe side clearly more expensive than
-- broadcasting the filtered result of `t_rc_dim`.
CREATE TABLE t_rc_probe (k UInt32, x String) ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
CREATE TABLE t_rc_dim (k UInt32 STATISTICS(uniq), v UInt32 STATISTICS(uniq), p UInt64)
  ENGINE = MergeTree ORDER BY k
  SETTINGS auto_statistics_types = '';
INSERT INTO t_rc_probe SELECT number, repeat('x', 100) FROM numbers(100000);
INSERT INTO t_rc_dim SELECT number, number % 1000, number FROM numbers(100000);

-- The reference pins the whole plan: `t_rc_dim` is read once in parallel (no `ReplicatedRead`)
-- and its filtered result is broadcast.
SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET max_rows_to_group_by = 0;
EXPLAIN SELECT sum(a.k + b.p), any(a.x) FROM t_rc_probe AS a INNER JOIN t_rc_dim AS b ON a.k = b.k WHERE b.v = 42;

DROP TABLE t_rc_dim;
DROP TABLE t_rc_probe;
