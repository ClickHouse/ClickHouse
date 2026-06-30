-- Derivative of 01308_row_policy_and_trivial_count_query with parallel replicas and plan serialization

SET optimize_move_to_prewhere = 1;
SET enable_analyzer = 1;
SET serialize_query_plan = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan=0; -- to ensure plan serialization/deserialization will happen

DROP TABLE IF EXISTS t;

CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t VALUES (1), (2), (3);

SELECT count() FROM t;
DROP ROW POLICY IF EXISTS filter ON t;
CREATE ROW POLICY filter ON t USING (x % 2 = 1) TO ALL;
SELECT count() FROM t;
DROP ROW POLICY filter ON t;
SELECT count() FROM t;

DROP TABLE t;
