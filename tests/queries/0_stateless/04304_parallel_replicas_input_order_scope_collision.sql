-- Companion to 04278_parallel_replicas_coordinator_mode_collision.
--
-- The parallel-replicas coordination mode is a pure function of `query_info.input_order_info`, which is
-- set only by `ReadFromMergeTree::requestReadingInOrder`. Several independent optimizations can call it:
-- read-in-order (ORDER BY), aggregation-in-order (GROUP BY, covered by 04278), distinct-in-order, and the
-- window-function storage-ordering reuse. For each of them, enabling the optimization in the outer scope
-- while disabling it in the subquery used to make the initiator's local plan announce `WithOrder` while
-- the remote replicas (which re-plan the subquery with its own settings) announce `Default` for the same
-- stream, throwing `Coordination mode mismatch`. The local plan must derive the read-in-order decision
-- from the subquery's own settings, so each of these now agrees and just returns the correct result.

DROP TABLE IF EXISTS t_input_order_scope;

CREATE TABLE t_input_order_scope (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;

-- Two parts so the in-order read is non-trivial across replicas.
INSERT INTO t_input_order_scope SELECT number % 1000, number FROM numbers(300000);
INSERT INTO t_input_order_scope SELECT number % 1000, number FROM numbers(300000, 300000);

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;

-- distinct-in-order: 1000 distinct values of `a`.
SELECT count() FROM
    (SELECT DISTINCT a FROM t_input_order_scope SETTINGS optimize_distinct_in_order = 0)
SETTINGS optimize_distinct_in_order = 1;

-- read-in-order (ORDER BY): 5 rows.
SELECT count() FROM
    (SELECT a FROM t_input_order_scope ORDER BY a LIMIT 5 SETTINGS optimize_read_in_order = 0)
SETTINGS optimize_read_in_order = 1;

-- window-function storage-ordering reuse: all 600000 rows.
SELECT count() FROM
    (SELECT a, sum(b) OVER (PARTITION BY a ORDER BY a) AS w FROM t_input_order_scope
     SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 0)
SETTINGS query_plan_reuse_storage_ordering_for_window_functions = 1;

DROP TABLE t_input_order_scope;
