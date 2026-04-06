-- Regression test for inconsistent AST formatting when GROUP BY combines
-- CUBE, ROLLUP, and TOTALS modifiers. The formatter outputs separate
-- WITH ROLLUP / WITH CUBE / WITH TOTALS clauses, but the parser previously
-- could not parse back multiple WITH clauses.
-- https://github.com/ClickHouse/ClickHouse/issues/101173

-- Single modifiers: baseline — formatting roundtrip must be idempotent.
SELECT formatQuery('SELECT 1 GROUP BY 1, 2, 3 WITH ROLLUP')
     = formatQuery(formatQuery('SELECT 1 GROUP BY 1, 2, 3 WITH ROLLUP')) AS rollup_only;

SELECT formatQuery('SELECT 1 GROUP BY 1, 2, 3 WITH CUBE')
     = formatQuery(formatQuery('SELECT 1 GROUP BY 1, 2, 3 WITH CUBE')) AS cube_only;

SELECT formatQuery('SELECT 1 GROUP BY 1, 2, 3 WITH TOTALS')
     = formatQuery(formatQuery('SELECT 1 GROUP BY 1, 2, 3 WITH TOTALS')) AS totals_only;

SELECT formatQuery('SELECT 1 GROUP BY ROLLUP(1, 2, 3)')
     = formatQuery(formatQuery('SELECT 1 GROUP BY ROLLUP(1, 2, 3)')) AS rollup_prefix;

SELECT formatQuery('SELECT 1 GROUP BY CUBE(1, 2, 3)')
     = formatQuery(formatQuery('SELECT 1 GROUP BY CUBE(1, 2, 3)')) AS cube_prefix;

-- Combined modifiers: the previously failing cases.
-- The formatter outputs "WITH ROLLUP WITH CUBE" which the parser must accept.
SELECT formatQuery('SELECT 1 GROUP BY ROLLUP(1, 2, 3) WITH CUBE')
     = formatQuery(formatQuery('SELECT 1 GROUP BY ROLLUP(1, 2, 3) WITH CUBE')) AS rollup_with_cube_idempotent;

SELECT formatQuery('SELECT 1 GROUP BY CUBE(1, 2, 3) WITH ROLLUP')
     = formatQuery(formatQuery('SELECT 1 GROUP BY CUBE(1, 2, 3) WITH ROLLUP')) AS cube_with_rollup_idempotent;

-- Three modifiers: WITH ROLLUP WITH CUBE WITH TOTALS
SELECT formatQuery('SELECT 1 GROUP BY 1 WITH ROLLUP WITH CUBE WITH TOTALS')
     = formatQuery(formatQuery('SELECT 1 GROUP BY 1 WITH ROLLUP WITH CUBE WITH TOTALS')) AS three_modifiers_idempotent;

-- Order independence: different orderings of modifiers produce the same formatted output
SELECT formatQuery('SELECT 1 GROUP BY 1 WITH CUBE WITH ROLLUP')
     = formatQuery('SELECT 1 GROUP BY 1 WITH ROLLUP WITH CUBE') AS order_independent;

-- Combined modifiers are parsed but throw NOT_IMPLEMENTED at execution time
SELECT 1 GROUP BY 1 WITH ROLLUP WITH CUBE; -- { serverError NOT_IMPLEMENTED }
SELECT 1 GROUP BY CUBE(1) WITH ROLLUP; -- { serverError NOT_IMPLEMENTED }
SELECT 1 GROUP BY ROLLUP(1) WITH CUBE; -- { serverError NOT_IMPLEMENTED }

-- Duplicate modifiers should be rejected as syntax errors
SELECT 1 GROUP BY 1 WITH ROLLUP WITH ROLLUP; -- { clientError SYNTAX_ERROR }
SELECT 1 GROUP BY 1 WITH CUBE WITH CUBE; -- { clientError SYNTAX_ERROR }
SELECT 1 GROUP BY 1 WITH TOTALS WITH TOTALS; -- { clientError SYNTAX_ERROR }
