SELECT toNullable('2023-02-09'::Date + number * 10) AS d FROM numbers(2) ORDER BY d WITH FILL;
SELECT '---';
SELECT number % 2 ? NULL : toNullable('2023-02-09'::Date + number) AS d FROM numbers(5) ORDER BY d ASC NULLS LAST WITH FILL;
-- TODO: NULLS FIRST does not work correctly with FILL.
