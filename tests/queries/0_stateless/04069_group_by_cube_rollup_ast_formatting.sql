-- Regression test for inconsistent AST formatting when both GROUP BY CUBE and
-- WITH ROLLUP (or vice versa) are combined.  The formatter used to emit
-- "WITH ROLLUP WITH CUBE" which the parser cannot re-parse, causing
-- "Inconsistent AST formatting" in debug builds.
-- https://github.com/ClickHouse/ClickHouse/issues/101173

SET max_execution_time = 30;

-- Single modifiers: baseline — formatting roundtrip must be idempotent.
SELECT formatQuery('SELECT 1 GROUP BY 1, 2, 3 WITH ROLLUP')
     = formatQuery(formatQuery('SELECT 1 GROUP BY 1, 2, 3 WITH ROLLUP')) AS rollup_only;

SELECT formatQuery('SELECT 1 GROUP BY 1, 2, 3 WITH CUBE')
     = formatQuery(formatQuery('SELECT 1 GROUP BY 1, 2, 3 WITH CUBE')) AS cube_only;

SELECT formatQuery('SELECT 1 GROUP BY ROLLUP(1, 2, 3)')
     = formatQuery(formatQuery('SELECT 1 GROUP BY ROLLUP(1, 2, 3)')) AS rollup_prefix;

SELECT formatQuery('SELECT 1 GROUP BY CUBE(1, 2, 3)')
     = formatQuery(formatQuery('SELECT 1 GROUP BY CUBE(1, 2, 3)')) AS cube_prefix;

-- Combined modifiers: the failing cases.
-- GROUP BY CUBE (...) WITH ROLLUP  =>  both flags set
SELECT formatQuery('SELECT 1 GROUP BY CUBE(1, 2, 3) WITH ROLLUP') AS cube_with_rollup;

SELECT formatQuery('SELECT 1 GROUP BY CUBE(1, 2, 3) WITH ROLLUP')
     = formatQuery(formatQuery('SELECT 1 GROUP BY CUBE(1, 2, 3) WITH ROLLUP')) AS cube_rollup_idempotent;

-- GROUP BY ROLLUP (...) WITH CUBE  =>  both flags set (other direction)
SELECT formatQuery('SELECT 1 GROUP BY ROLLUP(1, 2, 3) WITH CUBE') AS rollup_with_cube;

SELECT formatQuery('SELECT 1 GROUP BY ROLLUP(1, 2, 3) WITH CUBE')
     = formatQuery(formatQuery('SELECT 1 GROUP BY ROLLUP(1, 2, 3) WITH CUBE')) AS rollup_cube_idempotent;
