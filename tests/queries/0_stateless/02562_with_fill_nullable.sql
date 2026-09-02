SELECT toNullable('2023-02-09'::Date + number * 10) AS d FROM numbers(2) ORDER BY d WITH FILL;
SELECT '---';
SELECT number % 2 ? NULL : toNullable('2023-02-09'::Date + number) AS d FROM numbers(5) ORDER BY d ASC NULLS LAST WITH FILL;
-- TODO: NULLS FIRST does not work correctly with FILL.
SELECT '---';
SELECT number % 2 ? NULL : toNullable(toInt32(number)) AS x FROM numbers(2) ORDER BY x ASC WITH FILL FROM 1 TO 3;
SELECT '---';
-- The subquery and the pinned setting are what put DISTINCT in order above the fill; without both,
-- nothing checks that the generated rows are sorted.
SELECT DISTINCT * FROM
(
    SELECT number AS p, if(number < 100, NULL, toNullable(toInt32(number))) AS x
    FROM numbers(2) ORDER BY p ASC, x ASC WITH FILL FROM 1 TO 3
)
SETTINGS optimize_distinct_in_order = 1;
