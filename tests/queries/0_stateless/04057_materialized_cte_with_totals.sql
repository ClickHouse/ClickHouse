-- Reproducer for a bug where MaterializingCTETransform didn't drop totals/extremes,
-- causing Block structure mismatch when uniting CTE pipelines.
SET enable_materialized_cte = 1;
SET enable_analyzer = 1;

WITH
    cte1 AS MATERIALIZED (SELECT sum(number) FROM numbers(3) GROUP BY number % 2 WITH TOTALS),
    cte2 AS MATERIALIZED (SELECT sum(number) FROM numbers(5) GROUP BY number % 2 WITH TOTALS)
SELECT * FROM cte1 AS a, cte1 AS b, cte2 AS c, cte2 AS d ORDER BY ALL;
