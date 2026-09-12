-- { echo }

-- `allow_parallel_distinct` is off so that the final DISTINCT keeps the single stream: this checks
-- that the per-stream sortedness of the union reaches DISTINCT, and repartitioning by hash for the
-- parallel deduplication would reorder the result and hide it.
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
