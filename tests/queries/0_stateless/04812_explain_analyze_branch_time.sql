-- Since output for EXPLAIN ANALYZE is non-deterministic, we only assert
-- structural invariants via string matching on the `explain` column.

SET enable_analyzer = 1;

-- With `time = 1` the steps report the step/branch time and concurrency,
-- and every `Time` line is paired with a `Concurrency` line.
SELECT
    countIf(explain LIKE '%Time: step %· branch %') >= 1,
    countIf(explain LIKE '%Concurrency: step %· branch %') >= 1,
    countIf(explain LIKE '%Time: step %') = countIf(explain LIKE '%Concurrency: step %')
FROM (EXPLAIN ANALYZE time = 1
    SELECT number % 10 AS k, count() FROM numbers_mt(1000000) GROUP BY k);
