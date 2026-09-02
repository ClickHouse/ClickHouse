SELECT a.*, b.* FROM
(
    SELECT number AS k FROM system.numbers LIMIT 10
) AS a
ALL INNER JOIN
(
    SELECT intDiv(number, 2) AS k, number AS joined FROM system.numbers LIMIT 10
) AS b
USING k
ORDER BY k, joined;
