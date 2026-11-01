-- Verify that EXPLAIN AST INSERT ... with trailing FORMAT
-- on the EXPLAIN level formats correctly without wrapping INSERT in unparseable parens.
-- https://github.com/ClickHouse/ClickHouse/issues/100131

SELECT formatQuerySingleLine('EXPLAIN AST INSERT INTO t1 SELECT 1 FORMAT Null');
SELECT formatQuerySingleLine('EXPLAIN AST INSERT INTO t1 SELECT 1 INTERSECT DISTINCT WITH cte0 AS (SELECT 2) SELECT 3 FORMAT Null');
