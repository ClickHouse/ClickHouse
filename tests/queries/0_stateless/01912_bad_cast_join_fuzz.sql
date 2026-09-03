SELECT
    1023 + l,
    *
FROM
(
    SELECT toLowCardinality(toNullable(number)) AS l
    FROM system.numbers
    LIMIT 10
) AS s1
ANY LEFT JOIN
(
    SELECT toLowCardinality(toNullable(number)) AS r
    FROM system.numbers
    LIMIT 7
) AS s2 ON (l + 1023) = (r * 3)
ORDER BY l, r;
