-- https://github.com/ClickHouse/ClickHouse/issues/83442
-- Scalar subquery inside an unresolved table function argument should not cause
-- a LOGICAL_ERROR about unexpected IDENTIFIER node in extractTableExpressions.

SELECT * FROM remote('localhost', view(SELECT 2 AS x), concat(x, (SELECT 1)));
SELECT * FROM remote('localhost', view(SELECT 2 AS x), concat((SELECT 1), x));
