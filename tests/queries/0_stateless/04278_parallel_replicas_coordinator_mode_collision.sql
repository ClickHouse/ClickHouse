-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106039
--
-- A single query plan that reads the same table in two different parallel-replicas
-- coordination modes (one subquery requires `WithOrder`, the other `Default`) used
-- to throw `Coordination mode mismatch for stream <table>` because the coordinator
-- map was keyed by stream id alone. After the fix the two modes get independent
-- coordinator instances and the query succeeds.

DROP TABLE IF EXISTS t_pr_coord_collision;

CREATE TABLE t_pr_coord_collision (a String, b UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_pr_coord_collision
SELECT toString(rand() % 100000), number FROM numbers(300000);

INSERT INTO t_pr_coord_collision
SELECT toString(rand() % 100000), number FROM numbers(300000);

SET automatic_parallel_replicas_mode = 0;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;

-- The outer query carries `optimize_aggregation_in_order = 1`, which the parallel-
-- replicas planner uses to derive `WithOrder` for the standard-aggregator path. The
-- inner SETTINGS pin `optimize_aggregation_in_order = 0` on each subquery, but the
-- sharded-aggregator subquery still scatters rows by hash and ends up announcing
-- `Default`. Both subqueries share the same `ParallelReplicasReadingCoordinator`
-- instance, which is what the bug requires.
SELECT
    (SELECT count() FROM
        (SELECT a, sum(b) FROM t_pr_coord_collision GROUP BY a
         SETTINGS enable_sharding_aggregator = 0, optimize_aggregation_in_order = 0)) > 0,
    (SELECT count() FROM
        (SELECT a, sum(b) FROM t_pr_coord_collision GROUP BY a
         SETTINGS enable_sharding_aggregator = 1, optimize_aggregation_in_order = 0)) > 0
SETTINGS optimize_aggregation_in_order = 1;

DROP TABLE t_pr_coord_collision;
