-- To prevent old analyzer type checks
SET enable_analyzer=1;

EXPLAIN SYNTAX
WITH foo(1 AS `a0`) AS a
SELECT 1
FROM numbers() JOIN numbers() ON (1 AS a0);
