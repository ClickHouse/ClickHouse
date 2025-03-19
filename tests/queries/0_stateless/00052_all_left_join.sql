SELECT * FROM
(
    SELECT number AS k FROM system.numbers LIMIT 10
) js1
ALL LEFT JOIN
(
    SELECT intDiv(number, 2) AS k, number AS joined FROM system.numbers LIMIT 10
) js2
USING k
ORDER BY ALL;
