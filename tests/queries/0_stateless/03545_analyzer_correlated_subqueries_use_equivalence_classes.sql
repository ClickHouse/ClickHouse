SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_randomize = 0; -- Pinned because the test asserts on join plan/order
SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET query_plan_join_swap_table = 0; -- Changes query plan
SET correlated_subqueries_default_join_kind = 'left';
SET query_plan_remove_unused_columns = 1;
SET max_bytes_before_external_join = 0; -- Remove once spilling hash join is enabled by default

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
