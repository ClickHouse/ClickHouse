-- https://github.com/ClickHouse/ClickHouse/issues/118070
-- https://github.com/ClickHouse/ClickHouse/issues/103393
-- A GROUP BY key referenced from a window function was not converted to Nullable under
-- `group_by_use_nulls`, while the validator compared it against the Nullable key set, so the
-- reproducer queries below were rejected with NOT_AN_AGGREGATE.

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

SELECT '-- an aggregate inside a window function: the aggregate keeps the unconverted key, the window function sees the Nullable one';
SELECT k,
    sum(sum(x)) OVER (ORDER BY k) AS running_total_of_an_aggregate,
    any(any(toTypeName(k))) OVER (ORDER BY k) AS inside_the_nested_aggregate,
    any(toTypeName(k)) OVER (ORDER BY k) AS the_window_argument
FROM values('k String, x UInt8', ('a', 1), ('b', 2)) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- the same nesting under GROUPING SETS';
SELECT k, sum(sum(x)) OVER (ORDER BY k) AS running_total_of_an_aggregate,
    any(toTypeName(k)) OVER (ORDER BY k) AS the_window_argument
FROM values('k String, x UInt8', ('a', 1), ('b', 2)) GROUP BY GROUPING SETS ((k), ()) ORDER BY ALL;

SELECT '-- the same nesting under ROLLUP WITH TOTALS';
SELECT k, sum(sum(x)) OVER (ORDER BY k) AS running_total_of_an_aggregate,
    any(toTypeName(k)) OVER (ORDER BY k) AS the_window_argument
FROM values('k String, x UInt8', ('a', 1), ('b', 2)) GROUP BY k WITH ROLLUP WITH TOTALS ORDER BY ALL;

SELECT '-- the same nesting under CUBE WITH TOTALS';
SELECT k, sum(sum(x)) OVER (ORDER BY k) AS running_total_of_an_aggregate,
    any(toTypeName(k)) OVER (ORDER BY k) AS the_window_argument
FROM values('k String, x UInt8', ('a', 1), ('b', 2)) GROUP BY k WITH CUBE WITH TOTALS ORDER BY ALL;

SELECT '-- other window function names';
SELECT k,
    row_number() OVER (PARTITION BY k) AS a,
    dense_rank() OVER (PARTITION BY k) AS b,
    count() OVER (PARTITION BY k) AS c,
    lagInFrame(k) OVER (PARTITION BY k ORDER BY k) AS d
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- through the `*` matcher and an APPLY transformer, which carries its own aggregate-name guard';
SELECT * APPLY (x -> (min(x) OVER (), any(toTypeName(x)) OVER ()))
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

SELECT '-- an aggregate parameter folded from the key type now matches the same expression outside the window';
SELECT length(groupArray(toUInt8(isNullable(k)) + 1)(42) OVER ()) AS folded_parameter,
    toUInt8(isNullable(k)) + 1 AS outside_the_window
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- a frame offset folded from the key type now matches the named window, which always folded it this way';
SELECT count() OVER (ORDER BY 1 ROWS BETWEEN length(toTypeName(k)) - 5 PRECEDING AND CURRENT ROW) AS inline_frame,
    count() OVER w AS named_frame
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP
WINDOW w AS (ORDER BY 1 ROWS BETWEEN length(toTypeName(k)) - 5 PRECEDING AND CURRENT ROW) ORDER BY ALL;

SELECT '-- and a frame offset that is only constant while the key is not Nullable is rejected, as under a named window';
SELECT count() OVER (ORDER BY 1 ROWS BETWEEN toUInt8(k IS NULL) PRECEDING AND CURRENT ROW)
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP; -- { serverError BAD_ARGUMENTS }

-- Guards below: these already worked and must keep working unchanged.

SELECT '-- WITH TOTALS, where `group_by_use_nulls` does not reach the key today';
SELECT k, rank() OVER (PARTITION BY k) AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH TOTALS ORDER BY ALL;

SELECT '-- and an aggregate inside a window function under WITH TOTALS alone, unconverted for the same reason';
SELECT k, sum(sum(x)) OVER (ORDER BY k) AS running_total_of_an_aggregate,
    any(toTypeName(k)) OVER (ORDER BY k) AS the_window_argument
FROM values('k String, x UInt8', ('a', 1), ('b', 2)) GROUP BY k WITH TOTALS ORDER BY ALL;

SELECT '-- a named window, which resolves outside the window function and already worked';
SELECT k, row_number() OVER w AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP WINDOW w AS (PARTITION BY k) ORDER BY ALL;

SELECT '-- `grouping` keeps comparing its argument in the original form';
SELECT k, grouping(k) AS g, rank() OVER (PARTITION BY grouping(k)) AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- a nested aggregate in the window specification keeps its own argument unconverted';
SELECT k, rank() OVER (PARTITION BY min(k)) AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '===== every query above, with `group_by_use_nulls = 0`: no key is promoted, so none of them changes =====';
SET group_by_use_nulls = 0;

SELECT '-- window PARTITION BY over a ROLLUP key';
SELECT k, toTypeName(k) AS t, rank() OVER (PARTITION BY k) AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- the window really partitions';
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

SELECT '-- an aggregate inside a window function';
SELECT k,
    sum(sum(x)) OVER (ORDER BY k) AS running_total_of_an_aggregate,
    any(any(toTypeName(k))) OVER (ORDER BY k) AS inside_the_nested_aggregate,
    any(toTypeName(k)) OVER (ORDER BY k) AS the_window_argument
FROM values('k String, x UInt8', ('a', 1), ('b', 2)) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- the same nesting under GROUPING SETS';
SELECT k, sum(sum(x)) OVER (ORDER BY k) AS running_total_of_an_aggregate,
    any(toTypeName(k)) OVER (ORDER BY k) AS the_window_argument
FROM values('k String, x UInt8', ('a', 1), ('b', 2)) GROUP BY GROUPING SETS ((k), ()) ORDER BY ALL;

SELECT '-- the same nesting under ROLLUP WITH TOTALS';
SELECT k, sum(sum(x)) OVER (ORDER BY k) AS running_total_of_an_aggregate,
    any(toTypeName(k)) OVER (ORDER BY k) AS the_window_argument
FROM values('k String, x UInt8', ('a', 1), ('b', 2)) GROUP BY k WITH ROLLUP WITH TOTALS ORDER BY ALL;

SELECT '-- the same nesting under CUBE WITH TOTALS';
SELECT k, sum(sum(x)) OVER (ORDER BY k) AS running_total_of_an_aggregate,
    any(toTypeName(k)) OVER (ORDER BY k) AS the_window_argument
FROM values('k String, x UInt8', ('a', 1), ('b', 2)) GROUP BY k WITH CUBE WITH TOTALS ORDER BY ALL;

SELECT '-- other window function names';
SELECT k,
    row_number() OVER (PARTITION BY k) AS a,
    dense_rank() OVER (PARTITION BY k) AS b,
    count() OVER (PARTITION BY k) AS c,
    lagInFrame(k) OVER (PARTITION BY k ORDER BY k) AS d
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- through the `*` matcher and an APPLY transformer';
SELECT * APPLY (x -> (min(x) OVER (), any(toTypeName(x)) OVER ()))
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

SELECT '-- the resolved type of a window aggregate over the key';
SELECT k, toTypeName(min(k) OVER ()) AS t
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- an aggregate parameter folded from the key type';
SELECT length(groupArray(toUInt8(isNullable(k)) + 1)(42) OVER ()) AS folded_parameter,
    toUInt8(isNullable(k)) + 1 AS outside_the_window
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- a frame offset folded from the key type, beside the named window';
SELECT count() OVER (ORDER BY 1 ROWS BETWEEN length(toTypeName(k)) - 5 PRECEDING AND CURRENT ROW) AS inline_frame,
    count() OVER w AS named_frame
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP
WINDOW w AS (ORDER BY 1 ROWS BETWEEN length(toTypeName(k)) - 5 PRECEDING AND CURRENT ROW) ORDER BY ALL;

SELECT '-- and the frame offset that `group_by_use_nulls` makes non-constant is constant here, so it is accepted';
SELECT count() OVER (ORDER BY 1 ROWS BETWEEN toUInt8(k IS NULL) PRECEDING AND CURRENT ROW)
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP;

SELECT '-- WITH TOTALS';
SELECT k, rank() OVER (PARTITION BY k) AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH TOTALS ORDER BY ALL;

SELECT '-- an aggregate inside a window function under WITH TOTALS alone';
SELECT k, sum(sum(x)) OVER (ORDER BY k) AS running_total_of_an_aggregate,
    any(toTypeName(k)) OVER (ORDER BY k) AS the_window_argument
FROM values('k String, x UInt8', ('a', 1), ('b', 2)) GROUP BY k WITH TOTALS ORDER BY ALL;

SELECT '-- a named window';
SELECT k, row_number() OVER w AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP WINDOW w AS (PARTITION BY k) ORDER BY ALL;

SELECT '-- `grouping`';
SELECT k, grouping(k) AS g, rank() OVER (PARTITION BY grouping(k)) AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;

SELECT '-- a nested aggregate in the window specification';
SELECT k, rank() OVER (PARTITION BY min(k)) AS r
FROM values('k String', ('a'), ('b')) GROUP BY k WITH ROLLUP ORDER BY ALL;
