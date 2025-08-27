SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET enable_parallel_replicas = 0;

EXPLAIN actions = 1
SELECT 
    (SELECT
        count()
    FROM
        numbers(10)
    WHERE
        number = n.number)
FROM
    numbers(10) n;
