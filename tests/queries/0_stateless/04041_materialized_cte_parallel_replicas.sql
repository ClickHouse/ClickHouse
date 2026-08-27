SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

SET enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3;
SET parallel_replicas_local_plan = 1;
SET automatic_parallel_replicas_mode = 0;

CREATE TABLE users_04041 (uid Int16, name String, age Int16) ENGINE =
MergeTree ORDER BY uid;
INSERT INTO users_04041 VALUES (1231, 'John', 33), (6666, 'Ksenia', 48),
(8888, 'Alice', 50);

EXPLAIN WITH a AS MATERIALIZED (SELECT * FROM users_04041)
SELECT count() FROM a as l JOIN a as r ON l.uid = r.uid
SETTINGS enable_join_runtime_filters = 1;

-- Materialized CTE referenced twice (prevents inlining).
-- CTE subquery reads from MergeTree table → parallel replicas kicks in.
-- sendExternalTablesData tries to read CTE's StorageMemory before materialization.
WITH a AS MATERIALIZED (SELECT * FROM users_04041)
SELECT count() FROM a as l JOIN a as r ON l.uid = r.uid;

SET parallel_replicas_local_plan = 0;
-- Run the same query without local plan
WITH a AS MATERIALIZED (SELECT * FROM users_04041)
SELECT count() FROM a as l JOIN a as r ON l.uid = r.uid;

DROP TABLE IF EXISTS users_04041;
