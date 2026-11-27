SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

SELECT 1
FROM
    (SELECT 1 c0) AS tx
    JOIN
    (SELECT 1 c0) AS ty
    ON true
WHERE toInt32(tx.c0) = ty.c0;
