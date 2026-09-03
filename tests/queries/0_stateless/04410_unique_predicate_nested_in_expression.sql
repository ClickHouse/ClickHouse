SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_unique_nested;
CREATE TABLE t_unique_nested (a UInt64, b UInt64) ENGINE = Memory;
INSERT INTO t_unique_nested VALUES (1, 10), (2, 20), (3, 30);

-- The UNIQUE predicate folds to a constant, but the internal `__unique` node must never reach the
-- planner. When the predicate was nested inside another expression, the result constant kept the
-- `__unique` function as its source expression, and the planner recursed into the raw subquery
-- QueryNode and threw "Only correlated QueryNode can be used as action query tree node".

SELECT 'UNIQUE nested in an aggregate function in ORDER BY';
SELECT count() FROM t_unique_nested ORDER BY uniq(UNIQUE((SELECT number FROM numbers(3))), b);

SELECT 'UNIQUE nested in an aggregate function in SELECT';
SELECT uniq(UNIQUE((SELECT number FROM numbers(3))), b) FROM t_unique_nested;

SELECT 'UNIQUE nested in a scalar function';
SELECT 1 + UNIQUE((SELECT number FROM numbers(3)));
SELECT UNIQUE((SELECT number FROM numbers(3))) AND UNIQUE((SELECT 1 FROM numbers(3)));

SELECT 'UNIQUE with duplicate column names nested in an aggregate function';
SELECT uniq(UNIQUE((SELECT t.x, s.x FROM (SELECT 1 AS x FROM numbers(2)) AS t CROSS JOIN (SELECT 2 AS x) AS s)), b) FROM t_unique_nested;

SELECT 'Exact fuzzer reproducer: UNIQUE nested in an aggregate function in ORDER BY with duplicate columns';
SELECT uniq(b) FROM t_unique_nested ORDER BY uniq(UNIQUE((SELECT t.x, s.x FROM (SELECT 1 AS x FROM numbers(2)) AS t CROSS JOIN (SELECT 2 AS x) AS s)), b);

DROP TABLE t_unique_nested;
