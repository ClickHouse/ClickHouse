-- When the first argument is constant within a window frame, the true entropy H(A) is
-- zero, and the cached incremental sums in the window state make the computed value pure
-- rounding noise. Dividing by it used to amplify the noise beyond the debug sanity check
-- (an exception `Logical error: 'res < 1.0 + 1e-4'`) and could return an arbitrary value
-- in [0, 1] instead of 0 in release builds.

-- A constant first argument must always give 0, in every frame.
SELECT DISTINCT u
FROM
(
    SELECT n, theilsU(a, b) OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS u
    FROM (SELECT number AS n, 0 AS a, number % 7 AS b FROM numbers(10000))
)
WHERE n > 0 -- the single-row first frame is NaN by definition
ORDER BY u;

-- The AST fuzzer query that hit the sanity check: most frames accumulate a constant
-- first column (`intDivOrZero(10, number)` is 0 for all `number` > 10, and the frames
-- run from the highest group downwards).
WITH
    (SELECT theilsU(toUInt8(number % 10), toUInt8(number % -2147483649)) IGNORE NULLS FROM numbers(10000)) AS direct_raw,
    (SELECT theilsUMerge(st) OVER (ORDER BY g DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
     FROM
     (
        SELECT number % 1000 AS g, CAST(CAST(theilsUState(toUInt8(intDivOrZero(10, number)), toUInt8(number % -2147483648)), 'String'), 'AggregateFunction(theilsU, UInt8, UInt8)') AS st
        FROM numbers(10000)
        GROUP BY g
        ORDER BY g DESC
     )
     ORDER BY g ASC
     LIMIT 1) AS over_merged_raw
SELECT round(direct_raw, 4) AS direct, round(over_merged_raw, 4) AS merged_over;
