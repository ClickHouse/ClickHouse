SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET query_plan_join_swap_table = 0; -- Changes query plan
SET correlated_subqueries_default_join_kind = 'left';
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0; -- Disable automatic spilling for this test

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
