-- A merge join compares its keys with `compareAt`, which equates `-0.0` with `0.0` and every `NaN` with every
-- other one, while both the set operation and the hash join compare them bitwise. Executing the `DISTINCT`
-- modes as a join must not change which rows they return, so a float key keeps the set-operation step
-- whenever an algorithm that a merge join can be reached through is enabled.

SET enable_analyzer = 1;

SELECT 'a float key keeps -0.0 apart from 0.0';
SELECT count() FROM (SELECT 0.0::Float64 AS x INTERSECT DISTINCT SELECT -0.0::Float64) SETTINGS join_algorithm = 'partial_merge';
SELECT count() FROM (SELECT 0.0::Float64 AS x INTERSECT DISTINCT SELECT -0.0::Float64) SETTINGS join_algorithm = 'prefer_partial_merge';
SELECT count() FROM (SELECT 0.0::Float64 AS x INTERSECT DISTINCT SELECT -0.0::Float64) SETTINGS join_algorithm = 'auto';
SELECT count() FROM (SELECT 0.0::Float64 AS x INTERSECT DISTINCT SELECT -0.0::Float64) SETTINGS join_algorithm = 'hash';
SELECT count() FROM (SELECT 0.0::Float32 AS x EXCEPT DISTINCT SELECT -0.0::Float32) SETTINGS join_algorithm = 'prefer_partial_merge';

SELECT 'and so does a float nested in the key';
SELECT count() FROM (SELECT [0.0::Float64] AS x INTERSECT DISTINCT SELECT [-0.0::Float64]) SETTINGS join_algorithm = 'partial_merge';
SELECT count() FROM (SELECT (1, 0.0::Float32) AS x INTERSECT DISTINCT SELECT (1, -0.0::Float32)) SETTINGS join_algorithm = 'partial_merge';
SELECT count() FROM (SELECT 0.0::Nullable(Float64) AS x INTERSECT DISTINCT SELECT -0.0::Nullable(Float64)) SETTINGS join_algorithm = 'prefer_partial_merge';

SELECT 'the rewrite still applies to a key that compares the same either way';
SELECT trimLeft(explain) FROM (EXPLAIN PLAN SELECT * FROM (SELECT 1 AS x INTERSECT DISTINCT SELECT 1) SETTINGS join_algorithm = 'partial_merge', query_plan_join_swap_table = 'false')
WHERE explain LIKE '%Join%' OR explain LIKE '%IntersectOrExcept%';

SELECT 'a float key with only hash algorithms is rewritten too';
SELECT trimLeft(explain) FROM (EXPLAIN PLAN SELECT * FROM (SELECT 0.0::Float64 AS x INTERSECT DISTINCT SELECT 0.0::Float64) SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 'false')
WHERE explain LIKE '%Join%' OR explain LIKE '%IntersectOrExcept%';
