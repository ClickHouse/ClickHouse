-- Tags: no-parallel-replicas

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104376: unsorted
-- equal_ranges produced by ColumnNullable in a multi-column ORDER BY ... LIMIT. Many rows tie on
-- the leading columns; with a LIMIT smaller than the row count the limit shortcut drops the
-- mis-ordered equal ranges, so the result is sorted incorrectly.

-- Debug self-check: aborts the server with "Rows are not sorted with permutation" before the fix.
SELECT
    if(number % 3 = 0, ['a', 'b'], emptyArrayString())                          AS c1,
    if(number % 5 = 0, toNullable(toString(cityHash64(number, 10) % 4)), NULL)  AS c2,
    toString(cityHash64(number * 3, 10) % 12)                                   AS c3,
    if(number % 5 = 0, toNullable(toString(number % 3)), NULL)                  AS c4,
    toString(number % 4)                                                        AS c5
FROM numbers(1170)
ORDER BY ALL DESC NULLS FIRST
LIMIT 400
FORMAT Null;

-- Release coverage: the debug self-check above is compiled out on release builds, where the bug
-- surfaces as a silently wrong row order. Tag every output row with its position and assert that
-- no adjacent pair is out of DESC NULLS FIRST order. The key is NULL-safe (isNull is the
-- high-order component, so NULL sorts greatest as required by DESC NULLS FIRST), so every pair is
-- compared. Prints 1 with the fix, 0 with the bug.
WITH src AS
(
    SELECT
        if(number % 3 = 0, ['a', 'b'], emptyArrayString())                          AS c1,
        if(number % 5 = 0, toNullable(toString(cityHash64(number, 10) % 4)), NULL)  AS c2,
        toString(cityHash64(number * 3, 10) % 12)                                   AS c3,
        if(number % 5 = 0, toNullable(toString(number % 3)), NULL)                  AS c4,
        toString(number % 4)                                                        AS c5
    FROM numbers(1170)
),
sorted AS
(
    SELECT c1, c2, c3, c4, c5, rowNumberInAllBlocks() AS rn
    FROM (SELECT * FROM src ORDER BY ALL DESC NULLS FIRST LIMIT 400)
),
neighbors AS
(
    SELECT
        tuple(c1, isNull(c2), ifNull(c2, ''), c3, isNull(c4), ifNull(c4, ''), c5)                                AS cur,
        lagInFrame(tuple(c1, isNull(c2), ifNull(c2, ''), c3, isNull(c4), ifNull(c4, ''), c5)) OVER (ORDER BY rn) AS prev,
        rn
    FROM sorted
)
SELECT countIf(rn > 0 AND cur > prev) = 0 AS output_is_sorted
FROM neighbors
SETTINGS enable_analyzer = 1;
