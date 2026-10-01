-- Keep a shared captured lambda as a non-constant `ColumnFunction` across `FinishSorting`.
-- Each limited branch preserves a sort prefix; `UNION ALL` supplies two ordered streams.
SET optimize_read_in_order = 0;
SET optimize_sorting_by_input_stream_properties = 1;
SET query_plan_execute_functions_after_sorting = 1;
SET query_plan_optimize_lazy_materialization = 0;
SET query_plan_push_down_volume_reducing_functions = 0;
SET query_plan_filter_push_down = 0;
SET enable_optimize_predicate_expression = 0;
SET max_threads = 4;
SET max_block_size = 2;
SET max_parallel_ordered_merge_materialization_threads = 4;

CREATE VIEW parallel_ordered_merge_lambda_input AS
SELECT * FROM
(
    SELECT number % 2 AS prefix, number * 2 AS v, [number, number + 1] AS a
    FROM numbers_mt(8)
    ORDER BY prefix
    LIMIT 8
)
UNION ALL
SELECT * FROM
(
    SELECT number % 2 AS prefix, number * 2 + 1 AS v, [number, number + 1] AS a
    FROM numbers_mt(8)
    ORDER BY prefix
    LIMIT 8
);

-- The lambda is needed below sorting by `arraySum`, and above it by `arrayMap`.
SELECT
    countIf(explain LIKE '%MaterializeMergedDataTransform%') = 1,
    countIf(explain LIKE '%FinishSortingTransform%') = 1
FROM
(
    EXPLAIN PIPELINE compact = 1
    SELECT prefix, v, arrayMap(x -> x + v, a) AS mapped
    FROM parallel_ordered_merge_lambda_input
    ORDER BY prefix, arraySum(x -> x + v, a), v
);

-- The filter shares its captured lambda with the expression lifted above sorting.
SELECT
    countIf(explain LIKE '%MaterializeMergedDataTransform%') = 1,
    countIf(explain LIKE '%FinishSortingTransform%') = 1
FROM
(
    EXPLAIN PIPELINE compact = 1
    SELECT prefix, v, arrayMap(x -> x + v, a) AS mapped
    FROM parallel_ordered_merge_lambda_input
    WHERE arraySum(x -> x + v, a) > 10
    ORDER BY prefix, v DESC
);

-- Check values and row order through both serial and deferred materialization.
SET max_parallel_ordered_merge_materialization_threads = 0;

SELECT prefix, v, arrayMap(x -> x + v, a) AS mapped
FROM parallel_ordered_merge_lambda_input
ORDER BY prefix, arraySum(x -> x + v, a), v;

SELECT prefix, v, arrayMap(x -> x + v, a) AS mapped
FROM parallel_ordered_merge_lambda_input
WHERE arraySum(x -> x + v, a) > 10
ORDER BY prefix, v DESC;

SET max_parallel_ordered_merge_materialization_threads = 4;

SELECT prefix, v, arrayMap(x -> x + v, a) AS mapped
FROM parallel_ordered_merge_lambda_input
ORDER BY prefix, arraySum(x -> x + v, a), v;

SELECT prefix, v, arrayMap(x -> x + v, a) AS mapped
FROM parallel_ordered_merge_lambda_input
WHERE arraySum(x -> x + v, a) > 10
ORDER BY prefix, v DESC;

DROP VIEW parallel_ordered_merge_lambda_input;
