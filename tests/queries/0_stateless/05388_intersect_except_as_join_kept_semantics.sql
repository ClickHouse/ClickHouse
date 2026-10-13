-- INTERSECT DISTINCT and EXCEPT DISTINCT are executed as a join only when the join gives the same result as the
-- set operation: a hash join that reads all of both sides.

SET enable_analyzer = 1;

SELECT 'a merge join compares floats differently, so it keeps the set-operation step';
SELECT count() FROM (SELECT 0.0::Float64 AS x INTERSECT DISTINCT SELECT -0.0::Float64) SETTINGS join_algorithm = 'partial_merge';
SELECT count() FROM (SELECT 0.0::Float64 AS x INTERSECT DISTINCT SELECT -0.0::Float64) SETTINGS join_algorithm = 'hash,partial_merge';
SELECT count() FROM (SELECT 0.0::Float64 AS x INTERSECT DISTINCT SELECT -0.0::Float64) SETTINGS join_algorithm = 'hash';

SELECT 'the join limits would cut the right side short, so they keep the set-operation step';
SELECT * FROM (SELECT number FROM numbers(5) INTERSECT DISTINCT SELECT number FROM numbers(5)) ORDER BY ALL
SETTINGS max_rows_in_join = 1, join_overflow_mode = 'break';
SELECT * FROM (SELECT number FROM numbers(5) EXCEPT DISTINCT SELECT number FROM numbers(3)) ORDER BY ALL
SETTINGS max_bytes_in_join = 1, join_overflow_mode = 'break';

SELECT 'the limits of DISTINCT bound the result, not the inputs of the join';
SELECT count() FROM (SELECT number FROM numbers(5) EXCEPT DISTINCT SELECT number FROM numbers(3))
SETTINGS max_threads = 2, max_rows_in_distinct = 3, distinct_overflow_mode = 'throw';
SELECT count() FROM (SELECT number FROM numbers(10) INTERSECT DISTINCT SELECT number FROM numbers(5))
SETTINGS max_threads = 2, max_rows_in_distinct = 6, distinct_overflow_mode = 'throw';

SELECT 'executed as a join';
SELECT countIf(explain LIKE '%Join%') > 0
FROM (EXPLAIN SELECT number FROM numbers(5) INTERSECT DISTINCT SELECT number FROM numbers(3));
SELECT countIf(explain LIKE '%Join%') > 0
FROM (EXPLAIN SELECT number FROM numbers(5) INTERSECT DISTINCT SELECT number FROM numbers(3))
SETTINGS join_algorithm = 'grace_hash';
SELECT countIf(explain LIKE '%Join%') > 0
FROM (EXPLAIN SELECT number FROM numbers(5) INTERSECT DISTINCT SELECT number FROM numbers(3))
SETTINGS max_rows_in_join = 100;
