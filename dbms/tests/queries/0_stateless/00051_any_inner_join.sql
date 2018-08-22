SELECT * FROM
(
    SELECT number AS k FROM system.numbers LIMIT 10
)
ANY INNER JOIN
(
    SELECT number * 2 AS k, number AS joined FROM system.numbers LIMIT 10
)
USING k;
