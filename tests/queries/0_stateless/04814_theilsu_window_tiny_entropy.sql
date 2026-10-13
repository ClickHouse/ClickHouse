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

-- A genuinely non-constant first argument must not be mistaken for a constant one.
-- With counts {N - 1, 1} and N = 30 million, the true H(A) ≈ 6.1e-7 used to be below
-- the rounding-noise bound of the plain (uncompensated) cached incremental sums; with
-- the compensated sums it is handled by the ordinary window formula. Here B = A, so
-- H(A|B) = 0 and the true theilsU is 1, not 0. The states are pre-aggregated per group
-- and merged in the window, which keeps the test cheap: the first frame holds only the
-- constant group (theilsU = 0), the second adds the single-row group (theilsU = 1).
SET max_rows_to_read = 0; -- the test config limits reads to 20 million rows
SELECT round(u, 4) AS u
FROM
(
    SELECT theilsUMerge(st) OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS u
    FROM
    (
        SELECT number = 0 AS g, theilsUState(toUInt8(number = 0), toUInt8(number = 0)) AS st
        FROM numbers(30000000)
        GROUP BY g
    )
)
ORDER BY u;

-- The same near-noise-level H(A), but with a second argument that is not a function of
-- the first, so the true theilsU is strictly between 0 and 1. The window result must
-- stay on the amortized O(1) path (the exact-recomputation shortcut is only for frames
-- with a constant first argument, because it rescans the count maps) and must agree
-- with the exact aggregate code path.
WITH
    (SELECT theilsU(toUInt8(number = 0), toUInt16(number % 1000)) FROM numbers(30000000)) AS direct_raw,
    (SELECT theilsUMerge(st) OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
     FROM
     (
        SELECT number = 0 AS g, theilsUState(toUInt8(number = 0), toUInt16(number % 1000)) AS st
        FROM numbers(30000000)
        GROUP BY g
     )
     ORDER BY g DESC
     LIMIT 1) AS window_raw
SELECT round(direct_raw, 4) AS direct, round(window_raw, 4) AS windowed, abs(direct_raw - window_raw) < 1e-6;
