-- Tags: replica

DROP TABLE IF EXISTS t_window_const;
CREATE TABLE t_window_const (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_window_const SELECT toString(number) FROM numbers(2000);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0, prefer_localhost_replica = 0, enable_analyzer = 1;

-- A window function that does not reference the constant column projected by the inner
-- subquery used to crash with "Invalid number of columns in chunk pushed to OutputPort":
-- the coordinator computed an empty mergeable header while a replica streamed the constant column.
SELECT DISTINCT count(*) OVER () FROM (SELECT 0 FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT 0 AS c, s FROM t_window_const);
SELECT DISTINCT count(*) OVER (), 1 AS a, 'z' AS b FROM (SELECT 0, 5 FROM t_window_const);

DROP TABLE t_window_const;
