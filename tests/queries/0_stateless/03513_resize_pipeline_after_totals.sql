SET query_plan_merge_expressions = 1;

EXPLAIN PIPELINE
SELECT cityHash64(number)
FROM numbers_mt(100)
GROUP BY number
    WITH TOTALS
SETTINGS max_threads = 4;
