EXPLAIN SYNTAX
SELECT number
FROM numbers(10) AS SHUFFLE;

EXPLAIN PLAN
SELECT number
FROM numbers(10) AS SHUFFLE;

EXPLAIN PLAN
SELECT number
FROM numbers(10) AS SHUFFLE
LIMIT 3;

EXPLAIN SYNTAX
SELECT number
FROM numbers(10)
SHUFFLE
SETTINGS allow_experimental_shuffle_query = 1;

EXPLAIN PLAN
SELECT number
FROM numbers(10)
SHUFFLE
SETTINGS allow_experimental_shuffle_query = 1;

EXPLAIN PLAN
SELECT number
FROM numbers(10)
SHUFFLE
LIMIT 3
SETTINGS allow_experimental_shuffle_query = 1;

SELECT
    count(),
    uniqExact(number),
    min(number) >= 0,
    max(number) < 1000000
FROM
(
    SELECT number
    FROM numbers(1000000)
    SHUFFLE
    LIMIT 1000
    SETTINGS allow_experimental_shuffle_query = 1
);
