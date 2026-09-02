-- Tags: long, no-random-merge-tree-settings, no-distributed-cache
-- no-random-merge-tree-settings - times out in private

DROP TABLE IF EXISTS build;
DROP TABLE IF EXISTS skewed_probe;

SET session_timezone = 'UTC';

CREATE TABLE build ENGINE = MergeTree ORDER BY (key, begin)
AS
    SELECT
        toDateTime('1990-03-21 13:00:00') + INTERVAL number MINUTE AS begin,
        number % 4 AS key,
        number AS value
    FROM numbers(0, 4000000);

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
    FROM numbers(0, 4000000);

SET max_rows_to_read = 0;

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
