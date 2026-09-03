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
    SELECT DISTINCT x FROM
    (
        SELECT if(number = 0, NULL, toNullable(toInt32(5))) AS x FROM numbers(2)
        ORDER BY x ASC NULLS FIRST WITH FILL FROM 1 TO 3
    )
)
SETTINGS optimize_distinct_in_order = 1;
SELECT DISTINCT x FROM
(
    SELECT if(number = 0, NULL, toNullable(toInt32(5))) AS x FROM numbers(2)
    ORDER BY x ASC NULLS FIRST WITH FILL FROM 1 TO 3
)
SETTINGS optimize_distinct_in_order = 1;
SELECT '---';

SELECT if(number = 0, NULL, toNullable(toInt32(number))) AS x FROM numbers(3)
ORDER BY x ASC NULLS FIRST WITH FILL TO 5;
SELECT '---';

SELECT if(number = 0, NULL, toNullable(toInt32(0))) AS x FROM numbers(2)
ORDER BY x DESC NULLS FIRST WITH FILL FROM 3 TO 1 STEP -1;
