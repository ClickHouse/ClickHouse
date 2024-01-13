-- https://github.com/ClickHouse/ClickHouse/issues/46628
DROP TABLE IF EXISTS t;
CREATE TABLE t
(
    `n` int
)
    ENGINE = MergeTree
        ORDER BY n AS
SELECT *
FROM numbers(10);

SELECT
    sum(n),
    1 AS x
FROM t
GROUP BY x;

SELECT
    'processed' AS type,
    max(number) AS max_date,
    min(number) AS min_date
FROM numbers(100)
GROUP BY type;
