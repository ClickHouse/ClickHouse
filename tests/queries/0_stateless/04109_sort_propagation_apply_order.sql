-- { echo }

-- A `UNION ALL` of sorted inputs has stream order only. Serial final `DISTINCT` must deduplicate
-- across those streams without treating their concatenation as globally sorted.
SELECT DISTINCT g
FROM
(
    (SELECT number AS g FROM numbers(2) ORDER BY g)
    UNION ALL
    (SELECT number AS g FROM numbers(2) ORDER BY g)
)
SETTINGS allow_parallel_distinct = 0;

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
