SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_join_order_randomize = 0; -- Pinned because the test asserts on join plan/order
SET enable_analyzer=1;

SELECT explain
FROM (

EXPLAIN
SELECT *
FROM (
    SELECT a.number AS a_number, b.number AS b_number
    FROM numbers(10) AS a
        JOIN numbers(10) AS b
        ON a.number%2 = b.number%3
    ) AS ab
    JOIN numbers(10) AS c
    ON b_number = c.number+2
SETTINGS enable_join_runtime_filters=1, join_runtime_filter_min_probe_rows = 0

);
