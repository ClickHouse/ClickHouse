-- https://github.com/ClickHouse/ClickHouse/issues/46335
SET enable_analyzer = 1;
SELECT
    key_a + key_b AS d,
    rank() OVER () AS f
FROM
    (
        SELECT
            rand() % 10 AS key_a,
            rand(1) % 5 AS key_b,
            number
        FROM numbers(100)
        )
GROUP BY
    key_a,
    key_b
WITH ROLLUP
ORDER BY multiIf(d = 0, key_a, NULL) ASC
FORMAT Null;

SELECT
    key_a + key_b AS d,
    rank() OVER (PARTITION BY key_a + key_b) AS f
FROM
    (
        SELECT
            rand() % 10 AS key_a,
            rand(1) % 5 AS key_b,
            number
        FROM numbers(100)
        )
GROUP BY
    key_a,
    key_b
WITH ROLLUP
ORDER BY multiIf(d = 0, key_a, NULL) ASC
FORMAT Null;


SELECT
    grouping(key_a) + grouping(key_b) AS d,
    rank() OVER (PARTITION BY grouping(key_a) + grouping(key_b), multiIf(grouping(key_b) = 0, key_a, NULL)) AS f
FROM
    (
        SELECT
            rand() % 10 AS key_a,
            rand(1) % 5 AS key_b,
            number
        FROM numbers(100)
        )
GROUP BY
    key_a,
    key_b
WITH ROLLUP
ORDER BY multiIf(d = 0, key_a, NULL) ASC
FORMAT Null;

SELECT grouping(key_a) + grouping(key_b) AS d
FROM
    (
        SELECT
            rand() % 10 AS key_a,
            rand(toLowCardinality(1)) % 5 AS key_b,
            number
        FROM numbers(100)
        )
GROUP BY
    key_a,
    key_b
WITH ROLLUP
FORMAT Null;
