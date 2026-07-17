-- Tags: long

-- https://github.com/ClickHouse/ClickHouse/issues/21557

EXPLAIN SYNTAX
WITH
    x AS ( SELECT number FROM numbers(10) ),
    cross_sales AS (
        SELECT 1 AS xx
        FROM x, x AS d1, x AS d2, x AS d3, x AS d4, x AS d5, x AS d6, x AS d7, x AS d8, x AS d9
        WHERE x.number = d9.number
    )
SELECT xx FROM cross_sales WHERE xx = 2000 FORMAT Null;

SET max_analyze_depth = 1;

EXPLAIN SYNTAX
WITH
    x AS ( SELECT number FROM numbers(10) ),
    cross_sales AS (
        SELECT 1 AS xx
        FROM x, x AS d1, x AS d2, x AS d3, x AS d4, x AS d5, x AS d6, x AS d7, x AS d8, x AS d9
        WHERE x.number = d9.number
    )
SELECT xx FROM cross_sales WHERE xx = 2000;
