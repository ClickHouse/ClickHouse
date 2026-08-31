-- https://github.com/ClickHouse/ClickHouse/issues/116848
-- `query_plan_merge_expression_into_join` merges an expression step into the join graph, where
-- reordering can leave it computed twice from the raw inputs - once for the join key that decides
-- matching and once for the output column. A non-deterministic expression then draws independently
-- in the two places, and the returned rows violate the query's own `JOIN ON` condition.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_merge_expr_1;
DROP TABLE IF EXISTS t_merge_expr_2;
DROP TABLE IF EXISTS t_merge_expr_3;
CREATE TABLE t_merge_expr_1 (a UInt64) ENGINE = MergeTree ORDER BY a AS SELECT number FROM numbers(1000);
CREATE TABLE t_merge_expr_2 (b UInt64) ENGINE = MergeTree ORDER BY b AS SELECT number FROM numbers(1000);
CREATE TABLE t_merge_expr_3 (k UInt8) ENGINE = MergeTree ORDER BY k AS SELECT number FROM numbers(2);

-- The subquery emits 1000 rows with r in {0, 1} and t_merge_expr_3 holds exactly {0, 1}, so the join
-- must return exactly 1000 rows and every one of them must satisfy r = k.
SELECT countIf(r != k) AS on_violations, count() AS total
FROM (SELECT t_merge_expr_1.a AS a, rand() % 2 AS r FROM t_merge_expr_1 JOIN t_merge_expr_2 ON t_merge_expr_1.a = t_merge_expr_2.b) s
JOIN t_merge_expr_3 ON s.r = t_merge_expr_3.k;

SELECT countIf(r != k) AS on_violations, count() AS total
FROM (SELECT t_merge_expr_1.a AS a, rand() % 2 AS r FROM t_merge_expr_1 JOIN t_merge_expr_2 ON t_merge_expr_1.a = t_merge_expr_2.b) s
JOIN t_merge_expr_3 ON s.r = t_merge_expr_3.k
SETTINGS query_plan_merge_expression_into_join = 0;

-- A deterministic expression is still merged.
SELECT 'deterministic';
SELECT countIf(r != k) AS on_violations, count() AS total
FROM (SELECT t_merge_expr_1.a AS a, t_merge_expr_1.a % 2 AS r FROM t_merge_expr_1 JOIN t_merge_expr_2 ON t_merge_expr_1.a = t_merge_expr_2.b) s
JOIN t_merge_expr_3 ON s.r = t_merge_expr_3.k;

DROP TABLE t_merge_expr_1;
DROP TABLE t_merge_expr_2;
DROP TABLE t_merge_expr_3;
