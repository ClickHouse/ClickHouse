SET explain_query_plan_default = 'legacy';
SET enable_analyzer=1;
SET enable_parallel_replicas=0;
SET enable_join_runtime_filters=1;
SET query_plan_join_swap_table=0;
SET query_plan_remove_unused_columns=0; -- Explicitly disable to keep the behavior of the test unchanged

SELECT explain FROM
(
EXPLAIN keep_logical_steps=1, header=1
SELECT * FROM (SELECT number, number, number + 1 as e, e FROM numbers(5)) AS left
INNER JOIN (SELECT number, number + 1 as k, k FROM numbers(2, 5)) AS right
USING (number) ORDER BY ALL
);

SELECT * FROM (SELECT number, number, number + 1 as e, e FROM numbers(5)) AS left
INNER JOIN (SELECT number, number + 1 as k, k FROM numbers(2, 5)) AS right
USING (number) ORDER BY ALL;

SET query_plan_remove_unused_columns=1; -- And also run with it enabled to verify that the two optimizations work together

SELECT explain FROM
(
EXPLAIN keep_logical_steps=1, header=1
SELECT * FROM (SELECT number, number, number + 1 as e, e FROM numbers(5)) AS left
INNER JOIN (SELECT number, number + 1 as k, k FROM numbers(2, 5)) AS right
USING (number) ORDER BY ALL
);

SELECT * FROM (SELECT number, number, number + 1 as e, e FROM numbers(5)) AS left
INNER JOIN (SELECT number, number + 1 as k, k FROM numbers(2, 5)) AS right
USING (number) ORDER BY ALL;
