-- https://github.com/ClickHouse/ClickHouse/issues/104908
-- Scalar subquery on the left side of `IN` must be evaluated before `tuple` rewrite

SELECT (SELECT 1) IN tuple(x, 2)
FROM values('x Int32', 1, 2)
ORDER BY x;

SELECT (SELECT 1) NOT IN tuple(x, 2)
FROM values('x Int32', 1, 2)
ORDER BY x;
