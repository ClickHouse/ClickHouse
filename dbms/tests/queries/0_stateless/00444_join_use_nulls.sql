SET join_use_nulls = 0;

SELECT k, a, b
FROM
(
    SELECT number AS k, toString(number) AS a FROM system.numbers LIMIT 10
)
ANY INNER JOIN
(
    SELECT number AS k, toString(number) AS b FROM system.numbers LIMIT 5, 10
) USING (k) ORDER BY k;

SELECT k, a, b
FROM
(
    SELECT number AS k, toString(number) AS a FROM system.numbers LIMIT 10
)
ANY LEFT JOIN
(
    SELECT number AS k, toString(number) AS b FROM system.numbers LIMIT 5, 10
) USING (k) ORDER BY k;

SELECT k, a, b
FROM
(
    SELECT number AS k, toString(number) AS a FROM system.numbers LIMIT 10
)
ANY RIGHT JOIN
(
    SELECT number AS k, toString(number) AS b FROM system.numbers LIMIT 5, 10
) USING (k) ORDER BY k;

SELECT k, a, b
FROM
(
    SELECT number AS k, toString(number) AS a FROM system.numbers LIMIT 10
)
ANY FULL JOIN
(
    SELECT number AS k, toString(number) AS b FROM system.numbers LIMIT 5, 10
) USING (k) ORDER BY k;


SET join_use_nulls = 1;

SELECT k, a, b
FROM
(
    SELECT number AS k, toString(number) AS a FROM system.numbers LIMIT 10
)
ANY INNER JOIN
(
    SELECT number AS k, toString(number) AS b FROM system.numbers LIMIT 5, 10
) USING (k) ORDER BY k;

SELECT k, a, b
FROM
(
    SELECT number AS k, toString(number) AS a FROM system.numbers LIMIT 10
)
ANY LEFT JOIN
(
    SELECT number AS k, toString(number) AS b FROM system.numbers LIMIT 5, 10
) USING (k) ORDER BY k;

SELECT k, a, b
FROM
(
    SELECT number AS k, toString(number) AS a FROM system.numbers LIMIT 10
)
ANY RIGHT JOIN
(
    SELECT number AS k, toString(number) AS b FROM system.numbers LIMIT 5, 10
) USING (k) ORDER BY k;

SELECT k, a, b
FROM
(
    SELECT number AS k, toString(number) AS a FROM system.numbers LIMIT 10
)
ANY FULL JOIN
(
    SELECT number AS k, toString(number) AS b FROM system.numbers LIMIT 5, 10
) USING (k) ORDER BY k;
