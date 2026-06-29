-- Tags: replica

DROP TABLE IF EXISTS t_window_const;
CREATE TABLE t_window_const (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_window_const SELECT toString(number) FROM numbers(2000);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0, prefer_localhost_replica = 0, enable_analyzer = 1;

-- A window function that does not reference the constant column projected by the inner
-- subquery used to raise a LOGICAL_ERROR exception "Invalid number of columns in chunk
-- pushed to OutputPort": the coordinator computed an empty mergeable header while a replica
-- streamed the constant column.
SELECT DISTINCT count(*) OVER () FROM (SELECT 0 FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT 0 AS c, s FROM t_window_const);
SELECT DISTINCT count(*) OVER (), 1 AS a, 'z' AS b FROM (SELECT 0, 5 FROM t_window_const);

-- The projection is constant at runtime but not folded into a ConstantNode: identity is not
-- suitable for constant folding, yet it returns its constant argument unchanged (a ColumnConst).
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(0) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT identity(0) AS c, s FROM t_window_const);

-- A constant-output projection that references a source column: ignore(s) is not suitable
-- for constant folding (stays a FunctionNode) yet always returns a ColumnConst(UInt8).
SELECT DISTINCT count(*) OVER () FROM (SELECT ignore(s) FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT ignore(s) AS c, s FROM t_window_const);

-- UNION ALL subquery: each branch is delegated to the replicas as a plain projection
-- (SELECT 0 FROM t) while the Union/Window run on the coordinator, so the window is not
-- pushed into a const-projecting mergeable-state read and the header cannot diverge here.
SELECT count(*) OVER () FROM (SELECT 0 FROM t_window_const UNION ALL SELECT 0 FROM t_window_const) ORDER BY 1 LIMIT 1;
SELECT DISTINCT count(*) OVER () FROM (SELECT 0 AS c FROM t_window_const UNION ALL SELECT 1 AS c FROM t_window_const);
SELECT DISTINCT count(*) OVER () FROM (SELECT 0 AS c, s FROM t_window_const UNION ALL SELECT 0 AS c, s FROM t_window_const);

DROP TABLE t_window_const;
