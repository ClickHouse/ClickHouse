-- https://github.com/ClickHouse/ClickHouse/issues/21584
SELECT count()
FROM
(
    SELECT number AS key_1
    FROM numbers(15)
) AS x
ALL INNER JOIN
(
    SELECT number AS key_1
    FROM numbers(10)
) AS z ON key_1 = z.key_1;
