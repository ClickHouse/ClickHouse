-- { echo }

SELECT DISTINCT g
FROM
(
    (SELECT number AS g FROM numbers(2) ORDER BY g)
    UNION ALL
    (SELECT number AS g FROM numbers(2) ORDER BY g)
);

SELECT g, x
FROM
(
    SELECT g, x
    FROM
    (
        (SELECT number AS g, number AS x FROM numbers(2) ORDER BY g, x)
        UNION ALL
        (SELECT number AS g, number AS x FROM numbers(2) ORDER BY g, x)
    )
    LIMIT 1 BY g
)
ORDER BY g, x;

SELECT g, x
FROM
(
    (SELECT number AS g, number AS x FROM numbers(2) ORDER BY g, x)
    UNION ALL
    (SELECT number AS g, number AS x FROM numbers(2) ORDER BY g, x)
)
LIMIT -1 BY g;
