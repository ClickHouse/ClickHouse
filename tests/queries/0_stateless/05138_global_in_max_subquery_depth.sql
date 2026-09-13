-- https://github.com/ClickHouse/ClickHouse/issues/79345
-- The depth of a GLOBAL IN / GLOBAL JOIN subquery must count enclosing subqueries, not query tree nodes.

SET enable_analyzer = 1;
SET max_subquery_depth = 1;

SELECT count()
FROM remote('127.0.0.{1,2}', numbers(10))
WHERE number GLOBAL IN (SELECT number FROM numbers(10) GROUP BY number);

SELECT countIf(number GLOBAL IN (SELECT number FROM numbers(10) GROUP BY number))
FROM remote('127.0.0.{1,2}', numbers(10));

SELECT countIf(toUInt8(number GLOBAL IN (SELECT number FROM numbers(10) GROUP BY number)))
FROM remote('127.0.0.{1,2}', numbers(10));

-- Subqueries nested one level deeper need max_subquery_depth = 2, the same as without GLOBAL.
SELECT count()
FROM remote('127.0.0.{1,2}', numbers(10))
WHERE number IN (
    SELECT number
    FROM (SELECT number FROM numbers(10) GROUP BY number) AS l
    GLOBAL LEFT JOIN (SELECT number FROM numbers(10) GROUP BY number) AS r USING (number)
); -- { serverError TOO_DEEP_SUBQUERIES }

SELECT count()
FROM remote('127.0.0.{1,2}', numbers(10))
WHERE number IN (
    SELECT number FROM numbers(10)
    WHERE number GLOBAL IN (SELECT number FROM numbers(5) GROUP BY number)
); -- { serverError TOO_DEEP_SUBQUERIES }

SET max_subquery_depth = 2;

SELECT count()
FROM remote('127.0.0.{1,2}', numbers(10))
WHERE number IN (
    SELECT number
    FROM (SELECT number FROM numbers(10) GROUP BY number) AS l
    GLOBAL LEFT JOIN (SELECT number FROM numbers(10) GROUP BY number) AS r USING (number)
);

SELECT count()
FROM remote('127.0.0.{1,2}', numbers(10))
WHERE number IN (
    SELECT number FROM numbers(10)
    WHERE number GLOBAL IN (SELECT number FROM numbers(5) GROUP BY number)
);
