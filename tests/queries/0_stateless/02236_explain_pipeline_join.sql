SET query_plan_join_swap_table = false;
SET enable_analyzer = 1;
SET enable_parallel_replicas=0;

EXPLAIN PIPELINE
SELECT * FROM
(
    SELECT * FROM system.numbers LIMIT 10
) t1
ALL LEFT JOIN
(
    SELECT * FROM system.numbers LIMIT 10
) t2
USING number
SETTINGS max_threads=16;
