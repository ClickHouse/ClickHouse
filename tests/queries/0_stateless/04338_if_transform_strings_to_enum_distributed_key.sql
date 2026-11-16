-- Tags: distributed
-- Regression for https://github.com/ClickHouse/ClickHouse/issues/74716
-- optimize_if_transform_strings_to_enum used to fail with THERE_IS_NO_COLUMN over a Distributed
-- table or parallel replicas: the initiator kept the rewritten _CAST as a live function while the
-- shard folded it to a constant, so the action-node names diverged across the boundary.

DROP TABLE IF EXISTS t_04338_local;
DROP TABLE IF EXISTS t_04338_dist;

CREATE TABLE t_04338_local (n Int32) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_04338_local SELECT number FROM numbers(10);

CREATE TABLE t_04338_dist AS t_04338_local
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_04338_local);

SET optimize_if_transform_strings_to_enum = 1;

-- The exact shape reported in #74716: a plain projection, no GROUP BY / ORDER BY.
-- createLocalPlanForParallelReplicas (ParallelReplicasLocalPlan.cpp:102, the frame from the report)
-- runs converting actions for this shape too, so pin the original bug directly here. Row order
-- across shards is not stable, so FORMAT Null is used: the failure is a THERE_IS_NO_COLUMN
-- exception during planning, not a wrong result.
SELECT (n IN (0) ? 'type_in' : 'other') AS x FROM t_04338_dist FORMAT Null;

SELECT (n IN (0) ? 'type_in' : 'other') AS x FROM t_04338_dist FORMAT Null
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 2,
         cluster_for_parallel_replicas = 'test_cluster_two_shards_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT transform(n, [0, 1], ['x', 'y'], 'z') AS t FROM t_04338_dist FORMAT Null
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 2,
         cluster_for_parallel_replicas = 'test_cluster_two_shards_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1;

-- if as GROUP BY key
SELECT (n > 0 ? 'a' : 'b') AS x, count() FROM t_04338_dist GROUP BY x ORDER BY x;

-- if as ORDER BY key (no GROUP BY)
SELECT DISTINCT (n > 0 ? 'a' : 'b') AS x FROM t_04338_dist ORDER BY x;

-- transform as GROUP BY key
SELECT transform(n, [0, 1], ['x', 'y'], 'z') AS t, count() FROM t_04338_dist GROUP BY t ORDER BY t;

-- parallel replicas path (the configuration from the original report)
SELECT (n > 0 ? 'a' : 'b') AS x, count() FROM t_04338_dist GROUP BY x ORDER BY x
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 2,
         cluster_for_parallel_replicas = 'test_cluster_two_shards_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE t_04338_local;
DROP TABLE t_04338_dist;
