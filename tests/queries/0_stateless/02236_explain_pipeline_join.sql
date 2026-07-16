SET query_plan_join_swap_table = false;
SET enable_analyzer = 1;
SET enable_parallel_replicas=0;
SET query_plan_optimize_join_order_limit = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0; -- Disable automatic spilling for this test

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
