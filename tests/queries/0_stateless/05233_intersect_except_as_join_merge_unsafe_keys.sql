-- A merge join compares its keys with `compareAt`, while the set operation compares their bytes. The two disagree
-- for floats, for `JSON` and for the states of aggregate functions, and the join algorithms are tried in the order
-- they are listed in, so such a key keeps the set-operation step whenever a merge join is enabled at all.

SET enable_analyzer = 1;

SELECT 'a hash algorithm in the list does not make a float key safe';
SELECT count() FROM (SELECT 0.0::Float64 AS x INTERSECT DISTINCT SELECT -0.0::Float64) SETTINGS join_algorithm = 'partial_merge,hash';
SELECT count() FROM (SELECT 0.0::Float64 AS x INTERSECT DISTINCT SELECT -0.0::Float64) SETTINGS join_algorithm = 'hash,partial_merge';
SELECT count() FROM (SELECT 0.0::Float64 AS x INTERSECT DISTINCT SELECT -0.0::Float64) SETTINGS join_algorithm = 'prefer_partial_merge,hash';
SELECT count() FROM (SELECT 0.0::Float64 AS x INTERSECT DISTINCT SELECT -0.0::Float64) SETTINGS join_algorithm = 'auto,hash';
SELECT count() FROM (SELECT 0.0::Float64 AS x INTERSECT DISTINCT SELECT -0.0::Float64) SETTINGS join_algorithm = 'full_sorting_merge,hash';
SELECT count() FROM (SELECT 0.0::Float64 AS x EXCEPT DISTINCT SELECT -0.0::Float64) SETTINGS join_algorithm = 'full_sorting_merge,hash';

SELECT 'different states of an aggregate function stay different';
SELECT count() FROM
(
    SELECT uniqState(number) AS s FROM numbers(2) GROUP BY number
    INTERSECT DISTINCT
    SELECT uniqState(number + 10) AS s FROM numbers(2) GROUP BY number
) SETTINGS join_algorithm = 'partial_merge';
SELECT count() FROM
(
    SELECT uniqState(number) AS s FROM numbers(2) GROUP BY number
    EXCEPT DISTINCT
    SELECT uniqState(number + 10) AS s FROM numbers(2) GROUP BY number
) SETTINGS join_algorithm = 'full_sorting_merge,hash';

SELECT 'the step is kept for these keys with a merge join enabled';
SELECT trimLeft(explain) FROM (EXPLAIN PLAN SELECT * FROM (SELECT 0.0::Float64 AS x INTERSECT DISTINCT SELECT 0.0::Float64) SETTINGS join_algorithm = 'partial_merge,hash')
WHERE explain LIKE '%Join%' OR explain LIKE '%IntersectOrExcept%';
SELECT trimLeft(explain) FROM (EXPLAIN PLAN SELECT * FROM (SELECT uniqState(number) AS s FROM numbers(2) INTERSECT DISTINCT SELECT uniqState(number) FROM numbers(2)) SETTINGS join_algorithm = 'partial_merge,hash')
WHERE explain LIKE '%Join%' OR explain LIKE '%IntersectOrExcept%';
SELECT trimLeft(explain) FROM (EXPLAIN PLAN SELECT * FROM (SELECT '{"a" : 1}'::JSON AS j INTERSECT DISTINCT SELECT '{"a" : 1}'::JSON) SETTINGS join_algorithm = 'partial_merge,hash')
WHERE explain LIKE '%Join%' OR explain LIKE '%IntersectOrExcept%';

SELECT 'and the results with only hash algorithms enabled are the same as of the step';
SELECT count() FROM (SELECT uniqState(number) AS s FROM numbers(2) GROUP BY number INTERSECT DISTINCT SELECT uniqState(number) AS s FROM numbers(1, 2) GROUP BY number) SETTINGS join_algorithm = 'hash';
SELECT count() FROM (SELECT uniqState(number) AS s FROM numbers(2) GROUP BY number INTERSECT DISTINCT SELECT uniqState(number) AS s FROM numbers(1, 2) GROUP BY number) SETTINGS optimize_rewrite_intersect_except_to_join = 0;
SELECT count() FROM (SELECT '{"a" : 1}'::JSON AS j INTERSECT DISTINCT SELECT '{"a" : 1}'::JSON) SETTINGS join_algorithm = 'hash';
SELECT count() FROM (SELECT '{"a" : 1}'::JSON AS j EXCEPT DISTINCT SELECT '{"a" : 2}'::JSON) SETTINGS join_algorithm = 'hash';
