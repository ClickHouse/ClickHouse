SELECT left, right FROM
(
    SELECT number % 4 AS k1, toString(number % 3) AS k2, number AS left FROM system.numbers LIMIT 10
)
ALL LEFT JOIN
(
    SELECT number % 2 AS k1, toString(number % 6) AS k2, number AS right FROM system.numbers LIMIT 10
)
USING k1, k2;
