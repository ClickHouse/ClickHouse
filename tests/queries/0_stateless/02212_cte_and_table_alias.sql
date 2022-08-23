-- https://github.com/ClickHouse/ClickHouse/issues/19222
SET enable_global_with_statement = 1;

WITH t AS
         (
             SELECT number AS n
             FROM numbers(10000)
         )
SELECT count(*)
FROM t AS a
WHERE a.n < 5000;

WITH t AS
         (
             SELECT number AS n
             FROM numbers(10000)
         )
SELECT count(*)
FROM t AS a
WHERE t.n < 5000;


SET enable_global_with_statement = 0;

WITH t AS
         (
             SELECT number AS n
             FROM numbers(10000)
         )
SELECT count(*)
FROM t AS a
WHERE a.n < 5000;

WITH t AS
         (
             SELECT number AS n
             FROM numbers(10000)
         )
SELECT count(*)
FROM t AS a
WHERE t.n < 5000;


-- Check now that function aliases are included in the hash too
-- The 2 subqueries are different as the first column alias is changed and that modifies the second column value
SELECT
    (
        SELECT
                1 + 2 AS number,
                1 + number AS b
        FROM system.numbers
        LIMIT 10, 1
    ),
    (
        SELECT
                1 + 2 AS number2,
                1 + number AS b
        FROM system.numbers
        LIMIT 10, 1
    )
