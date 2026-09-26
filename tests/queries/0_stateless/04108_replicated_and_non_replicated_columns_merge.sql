SELECT * FROM
(
    SELECT n, m FROM (SELECT 2 AS n, map('a', toString(number)) AS m, [1] AS arr FROM numbers(1)) ARRAY JOIN arr
    UNION ALL
    SELECT 1 AS n, map('a', toString(number)) AS m FROM numbers(1)
)
ORDER BY n
FORMAT Null;
