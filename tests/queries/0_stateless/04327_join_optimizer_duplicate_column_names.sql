-- Regression test for STID 4752-593c: the join order optimizer must not throw
-- when a relation's output header contains duplicate column names (a subquery
-- projecting the same alias twice, kept distinct by a QUALIFY clause).
-- See https://github.com/ClickHouse/ClickHouse/pull/103088 for the analysis.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

-- Header with a within-side duplicate (__table1.x, __table1.y, __table1.x, __table1.y).
SELECT x FROM (SELECT 1 AS x, 2 AS y, x, y QUALIFY 7) LIMIT 100 FORMAT Null;

-- Multi-join chain so the duplicate header reaches the recursive optimizer path.
-- Pin the algorithm and limit so randomized stateless runs always exercise the
-- greedy chooseJoinOrder path (a limit of 0 or a dpsize-first list would skip it).
SELECT count() FROM
    (SELECT 1 AS x, 2 AS y, x, y QUALIFY 7) t1
    JOIN (SELECT number AS z FROM numbers(2)) t2 ON t1.x = t2.z
    JOIN (SELECT number AS w FROM numbers(2)) t3 ON t1.x = t3.w
    JOIN (SELECT number AS v FROM numbers(2)) t4 ON t1.x = t4.v
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_limit = 16
FORMAT Null;

-- Same shape under the dpsize algorithm, which takes a different reorder path.
SELECT count() FROM
    (SELECT 1 AS x, 2 AS y, x, y QUALIFY 7) t1
    JOIN (SELECT number AS z FROM numbers(2)) t2 ON t1.x = t2.z
    JOIN (SELECT number AS w FROM numbers(2)) t3 ON t1.x = t3.w
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', query_plan_optimize_join_order_limit = 16
FORMAT Null;
