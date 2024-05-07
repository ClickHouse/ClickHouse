-- Tags: long

-- https://s3.amazonaws.com/clickhouse-test-reports/55051/07f288862c56b0a98379a07101062689b0460788/stateless_tests_flaky_check__asan_.html

DROP TABLE IF EXISTS build;
DROP TABLE IF EXISTS skewed_probe;

CREATE TABLE build ENGINE = MergeTree ORDER BY (key, begin)
AS
    SELECT
        toDateTime('1990-03-21 13:00:00') + INTERVAL number MINUTE AS begin,
        number % 4 AS key,
        number AS value
    FROM numbers(0, 10000000);

CREATE TABLE skewed_probe ENGINE = MergeTree ORDER BY (key, begin)
AS
    SELECT
        toDateTime('1990-04-21 13:00:01') + INTERVAL number MINUTE AS begin,
        0 AS key
    FROM numbers(0, 5)
    UNION ALL
    SELECT
        toDateTime('1990-05-21 13:00:01') + INTERVAL number MINUTE AS begin,
        1 AS key
    FROM numbers(0, 10)
    UNION ALL
    SELECT
        toDateTime('1990-06-21 13:00:01') + INTERVAL number MINUTE AS begin,
        2 AS key
    FROM numbers(0, 20)
    UNION ALL
    SELECT
        toDateTime('1990-03-21 13:00:01') + INTERVAL number MINUTE AS begin,
        3 AS key
    FROM numbers(0, 10000000);


SELECT SUM(value), COUNT(*)
FROM skewed_probe
ASOF JOIN build
USING (key, begin)
;

SELECT SUM(value), COUNT(*)
FROM skewed_probe
ASOF JOIN build
USING (key, begin)
SETTINGS join_algorithm = 'full_sorting_merge'
;
