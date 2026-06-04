-- Regression test: GROUP BY CUBE(...) WITH ROLLUP caused inconsistent AST
-- formatting in debug builds because the formatted query could not be parsed back.
-- https://github.com/ClickHouse/ClickHouse/issues/100320

SET enable_analyzer = 1;

SELECT t0.* FROM (SELECT 1 AS v1, 2 AS v2, 'a' AS v3) AS t0 GROUP BY CUBE (v1, v2, v3) WITH ROLLUP; -- { serverError NOT_IMPLEMENTED }
SELECT 1 GROUP BY 1 WITH ROLLUP WITH CUBE; -- { serverError NOT_IMPLEMENTED }
SELECT 1 GROUP BY CUBE(1) WITH ROLLUP; -- { serverError NOT_IMPLEMENTED }
SELECT 1 GROUP BY ROLLUP(1) WITH CUBE; -- { serverError NOT_IMPLEMENTED }

-- Duplicate modifiers should be rejected
SELECT 1 GROUP BY 1 WITH ROLLUP WITH ROLLUP; -- { clientError SYNTAX_ERROR }
SELECT 1 GROUP BY 1 WITH CUBE WITH CUBE; -- { clientError SYNTAX_ERROR }
SELECT 1 GROUP BY 1 WITH TOTALS WITH TOTALS; -- { clientError SYNTAX_ERROR }
