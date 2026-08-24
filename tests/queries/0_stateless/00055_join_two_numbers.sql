SELECT left, right FROM
(
    SELECT number % 4 AS k1, number % 3 AS k2, number AS left FROM system.numbers LIMIT 10
) js1
ALL LEFT JOIN
(
    SELECT number % 2 AS k1, number % 6 AS k2, number AS right FROM system.numbers LIMIT 10
) js2
USING k1, k2
ORDER BY left, right;
