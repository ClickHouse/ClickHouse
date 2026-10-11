-- Repeated arguments must match repeated projection columns one-to-one.

SET enable_analyzer = 1;
SET optimize_uniq_to_count = 1;

SELECT 'duplicate argument does not match distinct projection';
SELECT uniqExact(x, x)
FROM
(
    SELECT DISTINCT
        number % 2 AS x,
        number AS y
    FROM numbers(4)
);
SELECT uniqExact(x, x)
FROM
(
    SELECT DISTINCT
        number % 2 AS x,
        number AS y
    FROM numbers(4)
)
SETTINGS optimize_uniq_to_count = 0;

SELECT 'duplicate argument after another match';
SELECT uniqExact(x, y, y)
FROM
(
    SELECT DISTINCT
        number % 2 AS x,
        number % 2 AS y,
        number AS z
    FROM numbers(4)
);
SELECT uniqExact(x, y, y)
FROM
(
    SELECT DISTINCT
        number % 2 AS x,
        number % 2 AS y,
        number AS z
    FROM numbers(4)
)
SETTINGS optimize_uniq_to_count = 0;

SELECT 'duplicate arguments also block group by rewrite';
SELECT uniqExact(x, x)
FROM
(
    SELECT
        number % 2 AS x,
        number AS y,
        count() AS c
    FROM numbers(4)
    GROUP BY x, y
)
WHERE y >= 0;
SELECT uniqExact(x, x)
FROM
(
    SELECT
        number % 2 AS x,
        number AS y,
        count() AS c
    FROM numbers(4)
    GROUP BY x, y
)
WHERE y >= 0
SETTINGS optimize_uniq_to_count = 0;

SELECT 'different argument order still matches';
SELECT uniqExact(y, x)
FROM
(
    SELECT DISTINCT
        number % 2 AS x,
        number AS y
    FROM numbers(4)
);
SELECT uniqExact(y, x)
FROM
(
    SELECT DISTINCT
        number % 2 AS x,
        number AS y
    FROM numbers(4)
)
SETTINGS optimize_uniq_to_count = 0;

SELECT count() > 0
FROM
(
    EXPLAIN QUERY TREE
    SELECT uniqExact(x, x)
    FROM
    (
        SELECT DISTINCT
            number % 2 AS x,
            number AS y
        FROM numbers(4)
    )
)
WHERE explain LIKE '%function_name: uniqExact%';

SELECT count() > 0
FROM
(
    EXPLAIN QUERY TREE
    SELECT uniqExact(x, x)
    FROM
    (
        SELECT
            number % 2 AS x,
            number AS y,
            count() AS c
        FROM numbers(4)
        GROUP BY x, y
    )
    WHERE y >= 0
)
WHERE explain LIKE '%function_name: uniqExact%';

SELECT count() > 0
FROM
(
    EXPLAIN QUERY TREE
    SELECT uniqExact(y, x)
    FROM
    (
        SELECT DISTINCT
            number % 2 AS x,
            number AS y
        FROM numbers(4)
    )
)
WHERE explain LIKE '%function_name: count%';
