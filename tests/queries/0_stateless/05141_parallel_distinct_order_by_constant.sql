-- A parallel final DISTINCT and a partitioned INTERSECT / EXCEPT hash-scatter their input into partition streams
-- that are consumed as they come. Sorting them by constants only merges the streams without buffering them first,
-- which must not deadlock: a k-way merge would wait on one partition while the scatter is blocked on another.

SET max_threads = 4;
-- The CI test config sets the global size limits, which disable the parallel final DISTINCT.
SET max_rows_in_distinct = 0, max_bytes_in_distinct = 0;
SET query_plan_remove_redundant_sorting = 0;

SELECT count(), sum(x) FROM (SELECT isNull(number) AS x FROM (SELECT DISTINCT number FROM numbers_mt(1000000)) ORDER BY x);
SELECT count(), sum(x) FROM (SELECT isNull(number) AS x FROM (SELECT number FROM numbers_mt(1000000) INTERSECT ALL SELECT number FROM numbers_mt(1000000)) ORDER BY x);
SELECT count(), sum(x) FROM (SELECT isNull(number) AS x FROM (SELECT DISTINCT number FROM numbers_mt(1000000) EXCEPT DISTINCT SELECT number FROM numbers(1000)) ORDER BY x);

SELECT explain FROM (EXPLAIN PIPELINE SELECT isNull(number) AS x FROM (SELECT DISTINCT number FROM numbers_mt(1000000)) ORDER BY x)
WHERE explain LIKE '%Sort%' OR explain LIKE '%Resize%' OR explain LIKE '%Scatter%' OR explain LIKE '%Distinct%';
