CREATE TABLE analyzer_subquery_pk
ENGINE = MergeTree
ORDER BY (pk1, pk2)
SETTINGS index_granularity = 8
AS
SELECT
    10 pk1,
    number pk2,
    'test_' || toString(number) str
FROM numbers(20);

EXPLAIN indexes = 1
SELECT *
FROM analyzer_subquery_pk
WHERE pk1 <= 10 AND (pk2 IN (5))
SETTINGS allow_experimental_analyzer = 1;

EXPLAIN indexes = 1
SELECT *
FROM analyzer_subquery_pk
WHERE pk1 <= 10 AND (pk2 IN (SELECT 5))
SETTINGS allow_experimental_analyzer = 1;

EXPLAIN indexes = 1
SELECT *
FROM analyzer_subquery_pk
WHERE pk1 <= 10 AND (pk2 IN (SELECT number FROM numbers(5, 1)))
SETTINGS allow_experimental_analyzer = 1;