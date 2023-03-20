SELECT a.*, b.* FROM
(
    SELECT number AS k FROM system.numbers LIMIT 10
) AS a
ANY LEFT JOIN
(
    SELECT number * 2 AS k, number AS joined FROM system.numbers LIMIT 10
) AS b
USING k;
