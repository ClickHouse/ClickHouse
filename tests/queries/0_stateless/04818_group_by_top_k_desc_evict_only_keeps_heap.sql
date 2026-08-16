-- `ORDER BY k DESC` over a key-ascending stream cannot reject any input rows:
-- every new key replaces the heap boundary. Skip this direction rather than
-- paying heap and hash-table churn through the full input.

SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_group_by_top_k_optimization = 1;
SET optimize_trivial_group_by_limit_query = 0;
SET max_threads = 1;
SET enable_parallel_replicas = 0;
SET optimize_aggregation_in_order = 0;

SELECT 'desc_skips_top_k';
SELECT countIf(explain LIKE '%Top-K:%') FROM
(
    EXPLAIN PLAN
    SELECT k, uniqExact(v)
    FROM (SELECT intDiv(number, 2) AS k, number % 7 AS v FROM numbers(1000000))
    GROUP BY k
    ORDER BY k DESC
    LIMIT 10
);

-- Results are unaffected: the top 10 keys with their complete aggregates.
SELECT 'results';
SELECT k, count(), uniqExact(v)
FROM (SELECT intDiv(number, 2) AS k, number % 7 AS v FROM numbers(1000000))
GROUP BY k
ORDER BY k DESC
LIMIT 10;
