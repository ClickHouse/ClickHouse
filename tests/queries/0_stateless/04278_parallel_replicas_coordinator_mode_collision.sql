-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106039

DROP TABLE IF EXISTS t_pr_coord_collision;

CREATE TABLE t_pr_coord_collision (a String, b UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_pr_coord_collision
SELECT toString(rand() % 100000), number FROM numbers(300000);

INSERT INTO t_pr_coord_collision
SELECT toString(rand() % 100000), number FROM numbers(300000);

SET automatic_parallel_replicas_mode = 0;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;

-- The outer query carries `optimize_aggregation_in_order = 1`, which the parallel-replicas
-- planner would use to derive `WithOrder` for the subqueries' reads if it planned them with the
-- outer scope's settings; each subquery's own SETTINGS pin `optimize_aggregation_in_order = 0`,
-- so a correct plan announces `Default`. Both subqueries read the same table and share the same
-- `ParallelReplicasReadingCoordinator` instance, which is what the bug required: a subquery
-- planned under the wrong settings scope announced a different mode to the already-created
-- coordinator.
SELECT
    (SELECT count() FROM
        (SELECT a, sum(b) FROM t_pr_coord_collision GROUP BY a
         SETTINGS optimize_aggregation_in_order = 0)) > 0,
    (SELECT count() FROM
        (SELECT a, sum(b) FROM t_pr_coord_collision GROUP BY a
         SETTINGS optimize_aggregation_in_order = 0)) > 0
SETTINGS optimize_aggregation_in_order = 1;

DROP TABLE t_pr_coord_collision;
