SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0; -- Changes query plan

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
