SELECT t1.k, t2.k
FROM
(
    SELECT number AS k
    FROM numbers(10)
) AS t1
INNER JOIN
(
    SELECT
        CAST(0, 'UInt64') AS k, k
    FROM numbers(3)
) AS t2 on t1.k = t2.k
ORDER BY t1.k, t2.k
;
