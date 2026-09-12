-- The planner lets the global order of the input survive the final DISTINCT, and the steps above may
-- rely on it, so an ordered input is merged instead of hash-scattered into partition streams.

SET max_threads = 4;
-- The CI test config sets the global size limits, which disable the parallel final DISTINCT.
SET max_rows_in_distinct = 0, max_bytes_in_distinct = 0;
SET optimize_distinct_in_order = 0, query_plan_remove_redundant_sorting = 0;

SELECT explain FROM (EXPLAIN PIPELINE SELECT DISTINCT a, b FROM (SELECT number % 1000 AS a, cityHash64(number) % 100000 AS b FROM numbers_mt(1000000) ORDER BY b LIMIT 500000) ORDER BY b)
WHERE explain LIKE '%Sort%' OR explain LIKE '%Scatter%' OR explain LIKE '%Distinct%';

SELECT groupArray(b) = arraySort(groupArray(b)) FROM (SELECT DISTINCT a, b FROM (SELECT number % 1000 AS a, cityHash64(number) % 100000 AS b FROM numbers_mt(1000000) ORDER BY b LIMIT 500000) ORDER BY b);
