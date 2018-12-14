SELECT * FROM
(
    SELECT number AS k FROM system.numbers LIMIT 10
)
ALL LEFT JOIN
(
    SELECT intDiv(number, 2) AS k, number AS joined FROM system.numbers LIMIT 10
)
USING k;
