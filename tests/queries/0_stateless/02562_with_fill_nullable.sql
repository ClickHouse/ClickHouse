SELECT toNullable('2023-02-09'::Date + number * 10) AS d FROM numbers(2) ORDER BY d WITH FILL;
SELECT '---';
SELECT number % 2 ? NULL : toNullable('2023-02-09'::Date + number) AS d FROM numbers(5) ORDER BY d ASC NULLS LAST WITH FILL;
-- TODO: NULLS FIRST does not work correctly with FILL.
SELECT '---';
SELECT number % 2 ? NULL : toNullable(toInt32(number)) AS x FROM numbers(2) ORDER BY x ASC WITH FILL FROM 1 TO 3;
SELECT '---';
-- DISTINCT in order above the fill checks the stream itself for contiguity, not only the
-- deduplicated result. Every DISTINCT stage has to be the in-order one: a hash stage below the
-- sorted one would drop a non-contiguous duplicate before the sorted one could see it.
SELECT (countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0)
   AND (countIf(explain ILIKE '%DistinctTransform%') = 0) FROM
(
    EXPLAIN PIPELINE
    SELECT DISTINCT * FROM
    (
        SELECT number AS p, if(number < 100, NULL, toNullable(toInt32(number))) AS x
        FROM numbers(2) ORDER BY p ASC, x ASC WITH FILL FROM 1 TO 3
    )
)
SETTINGS optimize_distinct_in_order = 1;
SELECT DISTINCT * FROM
(
    SELECT number AS p, if(number < 100, NULL, toNullable(toInt32(number))) AS x
    FROM numbers(2) ORDER BY p ASC, x ASC WITH FILL FROM 1 TO 3
)
SETTINGS optimize_distinct_in_order = 1;
SELECT '---';
-- With `use_with_fill_by_sorting_prefix = 0` an `ORDER BY` key before the fill key is an ordinary
-- column, so a generated row defaults it and that key, not the fill key, decides the row's position.
SELECT toInt32(number) - 2 AS p, number % 2 ? NULL : toNullable(toInt32(number)) AS x
FROM numbers(2) ORDER BY p ASC, x ASC NULLS LAST WITH FILL FROM 1 TO 2
SETTINGS use_with_fill_by_sorting_prefix = 0;
SELECT '---';
-- Same fill key with no ORDER BY key ahead of it: the fill key still decides placement, so the
-- setting being off changes nothing.
SELECT number % 2 ? NULL : toNullable(toInt32(number)) AS x
FROM numbers(2) ORDER BY x ASC WITH FILL FROM 1 TO 3
SETTINGS use_with_fill_by_sorting_prefix = 0;
SELECT '---';
-- `WITH FILL` fills the half-open range [FROM, TO), so equal bounds fill nothing. A range whose
-- first row holds `NULL` in the fill key is no exception.
SELECT if(number < 100, NULL, toNullable(toInt32(number))) AS x
FROM numbers(2) ORDER BY x ASC WITH FILL FROM 1 TO 1;
SELECT '---';
SELECT if(number < 100, NULL, toNullable(toInt32(number))) AS x
FROM numbers(2) ORDER BY x DESC WITH FILL FROM 1 TO 1;
SELECT '---';
-- An omitted `TO` bounds the range at the maximum `ORDER BY` value, which a `NULL` never is, so a
-- trailing `NULL` gets no generated rows in front of it.
SELECT number % 2 ? NULL : toNullable(toInt32(number)) AS x
FROM numbers(2) ORDER BY x ASC WITH FILL FROM 1;
SELECT '---';
SELECT if(number < 100, NULL, toNullable(toInt32(number))) AS x
FROM numbers(2) ORDER BY x ASC WITH FILL FROM 1;
