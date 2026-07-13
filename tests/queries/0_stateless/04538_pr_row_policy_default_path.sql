-- Confirms parallel replicas respects a row policy on the default path (serialize_query_plan=0),
-- where each replica receives the query as an AST, re-plans it and re-applies its own row policy.
-- Checked for both parallel_replicas_local_plan modes:
--   1 (default) - the initiator participates as a local replica;
--   0           - all reading happens on the remote replicas.
-- count() is used because the trivial-count optimization is disabled when a row policy exists,
-- so it forces a real read; sum(x) confirms the actual rows were filtered, not just the count.

SET enable_analyzer = 1; -- required for parallel replicas
SET serialize_query_plan = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t VALUES (1), (2), (3);

DROP ROW POLICY IF EXISTS filter ON t;
CREATE ROW POLICY filter ON t USING (x % 2 = 1) TO ALL; -- only odd x -> {1, 3}

SELECT count() FROM t SETTINGS parallel_replicas_local_plan = 1;
SELECT sum(x) FROM t SETTINGS parallel_replicas_local_plan = 1;
SELECT count() FROM t SETTINGS parallel_replicas_local_plan = 0;
SELECT sum(x) FROM t SETTINGS parallel_replicas_local_plan = 0;

DROP ROW POLICY filter ON t;
SELECT count() FROM t; -- policy removed -> all rows

DROP TABLE t;
