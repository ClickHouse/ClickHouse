-- Exercise WindowTransform frame-bound dispatch and the full set of built-in
-- window functions: ROWS vs RANGE frames, PRECEDING / FOLLOWING /
-- CURRENT ROW / UNBOUNDED, PARTITION BY, and rank/ntile/value helpers.

SELECT '--- ROWS frame: PRECEDING + CURRENT ROW ---';
SELECT number, sum(number) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
FROM numbers(5)
ORDER BY number;

SELECT '--- ROWS frame: PRECEDING + FOLLOWING ---';
SELECT number, avg(number) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
FROM numbers(5)
ORDER BY number;

SELECT '--- ROWS frame: UNBOUNDED PRECEDING + CURRENT ROW (running total) ---';
SELECT number, sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM numbers(5)
ORDER BY number;

SELECT '--- ROWS frame: CURRENT ROW + UNBOUNDED FOLLOWING ---';
SELECT number, sum(number) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
FROM numbers(5)
ORDER BY number;

SELECT '--- ROWS frame: PRECEDING + PRECEDING ---';
SELECT number, sum(number) OVER (ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
FROM numbers(5)
ORDER BY number;

SELECT '--- ROWS frame: FOLLOWING + FOLLOWING ---';
SELECT number, sum(number) OVER (ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)
FROM numbers(5)
ORDER BY number;

SELECT '--- RANGE frame: PRECEDING + FOLLOWING ---';
SELECT number, sum(number) OVER (ORDER BY number RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING)
FROM numbers(5)
ORDER BY number;

SELECT '--- RANGE frame: UNBOUNDED PRECEDING + CURRENT ROW ---';
SELECT number, sum(number) OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM numbers(5)
ORDER BY number;

SELECT '--- PARTITION BY with ORDER BY ---';
SELECT p, x, sum(x) OVER (PARTITION BY p ORDER BY x)
FROM (SELECT number % 2 AS p, number AS x FROM numbers(6))
ORDER BY p, x;

SELECT '--- row_number / rank / dense_rank / percent_rank / cume_dist ---';
-- row_number() needs a stable tiebreaker to be deterministic under ties.
-- rank() / dense_rank() need no tiebreaker so ties are visible.
-- percent_rank() / cume_dist() require an explicit UNBOUNDED..UNBOUNDED frame.
SELECT x, tb,
    row_number() OVER w_stable AS rn,
    rank() OVER w_x AS rnk,
    dense_rank() OVER w_x AS drnk,
    percent_rank() OVER w_full AS prank,
    cume_dist() OVER w_full AS cd
FROM (
    SELECT x, rowNumberInAllBlocks() AS tb
    FROM (SELECT arrayJoin([1, 1, 2, 3, 3, 4]) AS x)
)
WINDOW
    w_stable AS (ORDER BY x, tb),
    w_x      AS (ORDER BY x),
    w_full   AS (ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
ORDER BY x, tb;

SELECT '--- ntile ---';
SELECT x, ntile(3) OVER (ORDER BY x) FROM (SELECT arrayJoin([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]) AS x) ORDER BY x;

SELECT '--- first_value / last_value / nth_value ---';
SELECT x,
    first_value(x) OVER w,
    last_value(x) OVER w,
    nth_value(x, 2) OVER w
FROM (SELECT arrayJoin([10, 20, 30, 40]) AS x)
WINDOW w AS (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
ORDER BY x;

SELECT '--- lag / lead with default ---';
SELECT x,
    lagInFrame(x, 1, 0::UInt8) OVER (ORDER BY x),
    leadInFrame(x, 1, 0::UInt8) OVER (ORDER BY x)
FROM (SELECT arrayJoin([1::UInt8, 2, 3, 4, 5]) AS x)
ORDER BY x;

SELECT '--- empty input ---';
SELECT x, sum(x) OVER (ORDER BY x) FROM (SELECT 1 AS x WHERE 0);

SELECT '--- nullable input ---';
SELECT x, sum(x) OVER (ORDER BY x) FROM (
    SELECT CAST(1 AS Nullable(Int32)) AS x
    UNION ALL SELECT NULL
    UNION ALL SELECT 3
) ORDER BY x NULLS LAST;

SELECT '--- aggregate over window w/ String type ---';
SELECT x, count() OVER w, min(x) OVER w, max(x) OVER w, any(x) OVER w
FROM (SELECT arrayJoin(['a', 'b', 'c', 'd']) AS x)
WINDOW w AS (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
ORDER BY x;

SELECT '--- PARTITION BY + RANGE frame (group into stripes) ---';
SELECT p, x, sum(x) OVER (PARTITION BY p ORDER BY x RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)
FROM (SELECT number % 2 AS p, number AS x FROM numbers(8))
ORDER BY p, x;

SELECT '--- Errors: PRECEDING with negative offset ---';
SELECT sum(x) OVER (ORDER BY x ROWS BETWEEN -1 PRECEDING AND CURRENT ROW)
FROM (SELECT 1 AS x); -- { serverError SYNTAX_ERROR,BAD_ARGUMENTS,ILLEGAL_COLUMN,ILLEGAL_TYPE_OF_ARGUMENT }
