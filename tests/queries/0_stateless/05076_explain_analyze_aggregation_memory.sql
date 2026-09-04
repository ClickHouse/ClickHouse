-- EXPLAIN ANALYZE output is non-deterministic, so only the presence of the
-- hash table memory line under the Aggregating step and a non-zero value are checked.

SET enable_analyzer = 1;

SELECT
    countIf(explain LIKE '%Aggregating%') >= 1,
    countIf(explain LIKE '%Hash table: memory %') = 1,
    countIf(explain LIKE '%Hash table: memory %' AND explain NOT LIKE '%memory 0.00 B%' AND explain NOT LIKE '%memory 0 B%') = 1
FROM (EXPLAIN ANALYZE SELECT number % 1000 AS k, uniqExact(number) FROM numbers_mt(100000) GROUP BY k);
