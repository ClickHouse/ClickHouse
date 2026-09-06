-- https://github.com/ClickHouse/ClickHouse/issues/118070
-- https://github.com/ClickHouse/ClickHouse/issues/103393
-- A GROUP BY key referenced from a window function was not converted to Nullable under
-- `group_by_use_nulls`, while the validator compared it against the Nullable key set, so
-- every query below except the guards was rejected with NOT_AN_AGGREGATE.

SET enable_analyzer = 1;
SET group_by_use_nulls = 1;

SELECT '-- window PARTITION BY over a ROLLUP key, and the key is Nullable inside the window';
SELECT k, toTypeName(k) AS t, rank() OVER (PARTITION BY k) AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- the window really partitions: one row number per group, three without PARTITION BY';
SELECT countDistinct(r) FROM
    (SELECT row_number() OVER (PARTITION BY k) AS r
     FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP);
SELECT countDistinct(r) FROM
    (SELECT row_number() OVER () AS r
     FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP);

SELECT '-- window ORDER BY over a ROLLUP key';
SELECT k, rank() OVER (ORDER BY k) AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- the key as the argument of a window aggregate';
SELECT min(k) OVER () AS m
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- the key in both the argument and the partition';
SELECT min(k) OVER (PARTITION BY k) AS m
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- other window function names';
SELECT k,
    row_number() OVER (PARTITION BY k) AS a,
    dense_rank() OVER (PARTITION BY k) AS b,
    count() OVER (PARTITION BY k) AS c,
    lagInFrame(k) OVER (PARTITION BY k ORDER BY k) AS d
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- HAVING beside the window';
SELECT k, rank() OVER (PARTITION BY k) AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP HAVING k IS NOT NULL ORDER BY ALL;

SELECT '-- QUALIFY';
SELECT k, rank() OVER (PARTITION BY k) AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP QUALIFY r = 1 ORDER BY ALL;

SELECT '-- WITH CUBE';
SELECT k, rank() OVER (PARTITION BY k) AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH CUBE ORDER BY ALL;

SELECT '-- GROUPING SETS';
SELECT k, rank() OVER (PARTITION BY k) AS r, count() AS c
FROM values('k String', ('a'), ('b')) GROUP BY GROUPING SETS ((k), ()) ORDER BY ALL;

SELECT '-- the resolved type of a window aggregate over the key, on a query that was already accepted';
SELECT k, toTypeName(min(k) OVER ()) AS t
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

-- Guards below: these already worked and must keep working unchanged.

SELECT '-- WITH TOTALS, where `group_by_use_nulls` does not reach the key today';
SELECT k, rank() OVER (PARTITION BY k) AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH TOTALS ORDER BY ALL;

SELECT '-- a named window, which resolves outside the window function and already worked';
SELECT k, row_number() OVER w AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP WINDOW w AS (PARTITION BY k) ORDER BY ALL;

SELECT '-- `grouping` keeps comparing its argument in the original form';
SELECT k, grouping(k) AS g, rank() OVER (PARTITION BY grouping(k)) AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- a nested aggregate in the window specification keeps its own argument unconverted';
SELECT k, rank() OVER (PARTITION BY min(k)) AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- without `group_by_use_nulls`';
SELECT k, rank() OVER (PARTITION BY k) AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL
SETTINGS group_by_use_nulls = 0;
