-- Reproducer for https://github.com/ClickHouse/ClickHouse/issues/90583
-- GROUPING function should accept constant arguments that are in GROUP BY keys.

SELECT 'GROUPING SETS with constant alias';

SELECT
    x,
    y,
    SUM(y) AS sum_y,
    GROUPING(x, y) AS gx
FROM (SELECT 1 AS x, number AS y FROM numbers(3))
GROUP BY GROUPING SETS ((x, y), (x), (y), ())
ORDER BY gx, y;

SELECT 'Regular GROUP BY with constant alias';

SELECT
    x,
    GROUPING(x) AS gx
FROM (SELECT 1 AS x, number AS y FROM numbers(3))
GROUP BY x, y
ORDER BY y;
