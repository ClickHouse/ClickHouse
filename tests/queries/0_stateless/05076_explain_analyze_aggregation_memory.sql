-- EXPLAIN ANALYZE output is non-deterministic, so only the presence of the
-- hash table memory line under the Aggregating step is checked.

SET enable_analyzer = 1;

SELECT
    countIf(explain LIKE '%Aggregating%') >= 1,
    countIf(explain LIKE '%Hash table: memory %') = 1
FROM (EXPLAIN ANALYZE SELECT number % 1000 AS k, uniqExact(number) FROM numbers_mt(100000) GROUP BY k);
