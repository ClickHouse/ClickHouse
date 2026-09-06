SET explain_query_plan_default = 'legacy';
-- Pinned because the test asserts on join plan/order: CI randomizes the join-order
-- optimizer, and the algorithm / searched-plan budget / limit change the chosen order
-- independently of the randomize seed. Pin all of them to their defaults.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_optimize_join_order_max_searched_plans = 100000;
SET query_plan_optimize_join_order_limit = 10;
-- The join keys are computed expressions; the asserted plan shape expects them merged into the
-- JOIN step, so pin the merge against the runner's randomization.
SET query_plan_merge_expression_into_join = 1;
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
