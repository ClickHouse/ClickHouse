SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET enable_parallel_replicas = 0;

SELECT number
FROM numbers(10) AS t
WHERE exists((
    SELECT *
    FROM
    (
        SELECT number * 2 AS number
        FROM
        (
            SELECT number
            FROM numbers(6)
            WHERE (number + 2) < t.number
        )
    )
    WHERE number = t.number
))
ORDER BY number;
