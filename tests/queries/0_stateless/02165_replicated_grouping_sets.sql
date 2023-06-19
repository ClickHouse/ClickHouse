SELECT
    k1,
    k2,
    SUM(number) AS sum_value,
    count() AS count_value
FROM numbers(6)
GROUP BY
    GROUPING SETS
    (
        (number % 2 AS k1),
        (number % 3 AS k2)
    )
ORDER BY
    sum_value ASC,
    count_value ASC;

SELECT
    k1,
    k2,
    SUM(number) AS sum_value,
    count() AS count_value
FROM remote('127.0.0.{2,3}', numbers(6))
GROUP BY
    GROUPING SETS
    (
        (number % 2 AS k1),
        (number % 3 AS k2)
    )
ORDER BY
    sum_value ASC,
    count_value ASC;

SELECT
    k2,
    SUM(number) AS sum_value,
    count() AS count_value
FROM remote('127.0.0.{2,3}', numbers(6))
GROUP BY
    GROUPING SETS
    (
        (number % 3 AS k2)
    )
ORDER BY
    sum_value ASC,
    count_value ASC;
