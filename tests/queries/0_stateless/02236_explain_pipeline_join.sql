SET query_plan_join_swap_table = false;
SET enable_analyzer = 1;
SET enable_parallel_replicas=0;
SET query_plan_optimize_join_order_limit = 0;
SET query_plan_push_down_limit = 1;
SET query_plan_merge_expressions = 1;

EXPLAIN PIPELINE
SELECT * FROM
(
    SELECT * FROM system.numbers LIMIT 100000
) t1
ALL LEFT JOIN
(
    SELECT * FROM system.numbers LIMIT 100000
) t2
USING number
SETTINGS max_threads=16;
