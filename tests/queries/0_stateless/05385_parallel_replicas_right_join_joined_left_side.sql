-- A RIGHT JOIN whose left side is itself a join, inside a subquery of a query read with parallel
-- replicas. When shipping makes that RIGHT JOIN global, its left side would have to be materialized
-- into a temporary table, which is not possible for a join, so the query runs without parallel replicas.

DROP TABLE IF EXISTS t_a;
DROP TABLE IF EXISTS t_b;
DROP TABLE IF EXISTS t_merge;

CREATE TABLE t_a (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_a SELECT number FROM numbers(10);

CREATE TABLE t_b (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_b SELECT number FROM numbers(10);

-- Merge, not Log/Memory: the stress runner rewrites those to MergeTree.
CREATE TABLE t_merge (key UInt64) ENGINE = Merge(currentDatabase(), '^t_b$');

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1,
    max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

-- The subqueries select the left side's column: a lost or mis-joined left side shrinks the set.

SELECT '-- table function in the joined left side';
SELECT count() FROM (SELECT key FROM t_a WHERE key IN (
    SELECT c.key FROM numbers(10) AS a JOIN t_b AS b ON a.number = b.key RIGHT JOIN t_b AS c ON b.key = c.key)) AS l;

SELECT '-- table function in the joined left side, the table read with parallel replicas directly';
SELECT count() FROM t_a WHERE key IN (
    SELECT b.key FROM numbers(10) AS a JOIN t_b AS b ON a.number = b.key RIGHT JOIN t_b AS c ON b.key = c.key);

SELECT '-- parallel_replicas_prefer_local_join = 0';
SELECT count() FROM (SELECT key FROM t_a WHERE key IN (
    SELECT b.key FROM numbers(10) AS a JOIN t_b AS b ON a.number = b.key RIGHT JOIN t_b AS c ON b.key = c.key)) AS l
SETTINGS parallel_replicas_prefer_local_join = 0;

SELECT '-- comma join as the left side';
SELECT count() FROM (SELECT key FROM t_a WHERE key IN (
    SELECT b.key FROM numbers(10) AS a, t_b AS b RIGHT JOIN t_b AS c ON b.key = c.key)) AS l;

SELECT '-- explicit GLOBAL RIGHT JOIN';
SELECT count() FROM (SELECT key FROM t_a WHERE key IN (
    SELECT b.key FROM t_b AS a JOIN t_b AS b ON a.key = b.key GLOBAL RIGHT JOIN t_b AS c ON b.key = c.key)) AS l;

SELECT '-- storage not eligible for parallel replicas in the joined left side';
SELECT count() FROM (SELECT key FROM t_a WHERE key IN (
    SELECT b.key FROM t_b AS a JOIN t_merge AS b ON a.key = b.key RIGHT JOIN t_b AS c ON b.key = c.key)) AS l;

SELECT '-- only MergeTree in the joined left side: the join stays local and parallel replicas are used';
SELECT count() > 0 FROM (
    EXPLAIN SELECT count() FROM (SELECT key FROM t_a WHERE key IN (
        SELECT b.key FROM t_b AS a JOIN t_b AS b ON a.key = b.key RIGHT JOIN t_b AS c ON b.key = c.key)) AS l
    SETTINGS query_plan_join_swap_table = 0
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM (SELECT key FROM t_a WHERE key IN (
    SELECT b.key FROM t_b AS a JOIN t_b AS b ON a.key = b.key RIGHT JOIN t_b AS c ON b.key = c.key)) AS l
SETTINGS query_plan_join_swap_table = 0;

SELECT '-- table function in the joined left side: parallel replicas are not used';
SELECT count() FROM (
    EXPLAIN SELECT count() FROM (SELECT key FROM t_a WHERE key IN (
        SELECT c.key FROM numbers(10) AS a JOIN t_b AS b ON a.number = b.key RIGHT JOIN t_b AS c ON b.key = c.key)) AS l
    SETTINGS query_plan_join_swap_table = 0
) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';

SELECT '-- enable_parallel_replicas = 2 throws instead';
SELECT count() FROM (SELECT key FROM t_a WHERE key IN (
    SELECT c.key FROM numbers(10) AS a JOIN t_b AS b ON a.number = b.key RIGHT JOIN t_b AS c ON b.key = c.key)) AS l
SETTINGS enable_parallel_replicas = 2; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM t_a WHERE key IN (
    SELECT b.key FROM numbers(10) AS a JOIN t_b AS b ON a.number = b.key RIGHT JOIN t_b AS c ON b.key = c.key)
SETTINGS enable_parallel_replicas = 2; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_merge;
DROP TABLE t_b;
DROP TABLE t_a;
