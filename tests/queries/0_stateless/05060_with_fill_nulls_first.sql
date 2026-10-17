-- `NULLS FIRST` puts the `NULL`s at the head of a `WITH FILL` range, so no generated row may precede them.

SELECT if(number = 0, NULL, toNullable(toInt32(5))) AS x FROM numbers(2)
ORDER BY x ASC NULLS FIRST WITH FILL FROM 1 TO 3;
SELECT '---';

-- The gate keeps the next statement non-vacuous: only the in-order DISTINCT reads the stream itself for
-- contiguity, and a hash stage below it would drop a non-contiguous duplicate before that check runs.
SELECT (countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0)
   AND (countIf(explain ILIKE '%DistinctTransform%') = 0) FROM
(
    EXPLAIN PIPELINE
    SELECT DISTINCT p, x FROM
    (
        SELECT number AS p, CAST(NULL, 'Nullable(Int32)') AS x FROM numbers(2)
        ORDER BY p ASC NULLS LAST, x ASC NULLS FIRST WITH FILL FROM 1 TO 3
    )
)
SETTINGS optimize_distinct_in_order = 1;
SELECT DISTINCT p, x FROM
(
    SELECT number AS p, CAST(NULL, 'Nullable(Int32)') AS x FROM numbers(2)
    ORDER BY p ASC NULLS LAST, x ASC NULLS FIRST WITH FILL FROM 1 TO 3
)
SETTINGS optimize_distinct_in_order = 1;
SELECT '---';

SELECT if(number = 0, NULL, toNullable(toInt32(number))) AS x FROM numbers(3)
ORDER BY x ASC NULLS FIRST WITH FILL TO 5;
SELECT '---';

SELECT if(number = 0, NULL, toNullable(toInt32(0))) AS x FROM numbers(2)
ORDER BY x DESC NULLS FIRST WITH FILL FROM 3 TO 1 STEP -1;
SELECT '---';

-- `NULLS FIRST` sorts a `NaN` between the `NULL`s and the values, so it belongs to the same prefix.
SELECT * FROM values('x Nullable(Float64)', (5), (nan), (NULL))
ORDER BY x ASC NULLS FIRST WITH FILL FROM 1 TO 3;
SELECT '---';
SELECT * FROM values('x Nullable(Float64)', (nan), (NULL))
ORDER BY x ASC NULLS FIRST WITH FILL FROM 1 TO 3;
SELECT '---';

-- With no `FILL FROM` the first ordinary value anchors the range, so the gaps after it are still generated.
SELECT * FROM values('x Nullable(Float64)', (7), (3), (nan))
ORDER BY x ASC NULLS FIRST WITH FILL;
SELECT '---';

-- A range holding only prefix rows must emit each generated row after the prefix, exactly once.
SELECT * FROM values('x Nullable(Float64)', (nan))
ORDER BY x ASC NULLS FIRST WITH FILL FROM 1 TO 3;
SELECT '---';

-- The `FILL FROM` row must not precede the prefix when ordinary values follow it either.
SELECT * FROM values('x Nullable(Float64)', (nan), (5))
ORDER BY x ASC NULLS FIRST WITH FILL FROM 1 TO 3;
SELECT '---';

-- `NULLS FIRST` sorts a `NaN` below every value in a non-`Nullable` column too.
SELECT * FROM values('x Float64', (nan), (5))
ORDER BY x ASC NULLS FIRST WITH FILL FROM 1 TO 3;
SELECT '---';

-- A range starting with an ordinary value takes its `FILL FROM` row from the preamble, so the prefix
-- rules must not emit it again, neither in the first range nor in one following a prefix range.
SELECT * FROM values('x Nullable(Float64)', (5))
ORDER BY x ASC NULLS FIRST WITH FILL FROM 1 TO 3;
SELECT '---';
SELECT * FROM values('p UInt8, x Nullable(Float64)', (0, NULL), (1, 5))
ORDER BY p ASC NULLS LAST, x ASC NULLS FIRST WITH FILL FROM 1 TO 3;
SELECT '---';

-- A `NaN` bound is not an ordered position either, so the prefix rules stay off and a bound reaching the
-- filling row keeps generating exactly the rows it does without them.
SELECT * FROM values('x Nullable(Float64)', (3), (7))
ORDER BY x ASC NULLS FIRST WITH FILL FROM nan;
SELECT '---';
-- A `NaN` `FILL TO` never closes the range, so a range re-anchored under one would generate rows without
-- end. The memory limit bounds that to a failed query rather than an exhausted host.
SELECT * FROM values('x Nullable(Float64)', (nan), (3), (7))
ORDER BY x ASC NULLS FIRST WITH FILL TO nan
SETTINGS max_memory_usage = 536870912;
SELECT '---';

-- An infinity does not close the range either, and an infinite `FILL FROM` still emits its own row ahead
-- of the prefix, both exactly as they did before the rules above.
SELECT * FROM values('x Nullable(Float64)', (nan), (3), (7))
ORDER BY x ASC NULLS FIRST WITH FILL TO inf
SETTINGS max_memory_usage = 536870912;
SELECT '---';
SELECT * FROM values('x Nullable(Float64)', (nan), (3), (7))
ORDER BY x ASC NULLS FIRST WITH FILL FROM inf;
