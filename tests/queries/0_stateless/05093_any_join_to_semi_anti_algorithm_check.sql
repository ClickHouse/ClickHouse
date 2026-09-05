-- `query_plan_convert_any_join_to_semi_or_anti_join` turns the `ANY` join of a decorrelated `EXISTS`
-- into a `SEMI` or `ANTI` join. The sort-merge algorithms implement neither, so under
-- `join_algorithm = 'full_sorting_merge'` or `'partial_merge'` the conversion turned a query that
-- runs into `NOT_IMPLEMENTED`. The pass now converts only when an enabled algorithm can execute the
-- result.

SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS t_semi_anti_algo;
CREATE TABLE t_semi_anti_algo (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_semi_anti_algo SELECT number, number % 7 FROM numbers(100);

SELECT 'a correlated EXISTS under each algorithm';
SELECT count() FROM t_semi_anti_algo AS o WHERE EXISTS (SELECT 1 FROM t_semi_anti_algo AS i WHERE i.b = o.b AND i.a = 5) SETTINGS join_algorithm = 'full_sorting_merge';
SELECT count() FROM t_semi_anti_algo AS o WHERE EXISTS (SELECT 1 FROM t_semi_anti_algo AS i WHERE i.b = o.b AND i.a = 5) SETTINGS join_algorithm = 'partial_merge';
SELECT count() FROM t_semi_anti_algo AS o WHERE EXISTS (SELECT 1 FROM t_semi_anti_algo AS i WHERE i.b = o.b AND i.a = 5) SETTINGS join_algorithm = 'hash';

SELECT 'and a correlated NOT EXISTS';
SELECT count() FROM t_semi_anti_algo AS o WHERE NOT EXISTS (SELECT 1 FROM t_semi_anti_algo AS i WHERE i.b = o.b AND i.a = 5) SETTINGS join_algorithm = 'full_sorting_merge';
SELECT count() FROM t_semi_anti_algo AS o WHERE NOT EXISTS (SELECT 1 FROM t_semi_anti_algo AS i WHERE i.b = o.b AND i.a = 5) SETTINGS join_algorithm = 'partial_merge';
SELECT count() FROM t_semi_anti_algo AS o WHERE NOT EXISTS (SELECT 1 FROM t_semi_anti_algo AS i WHERE i.b = o.b AND i.a = 5) SETTINGS join_algorithm = 'hash';

-- The `IN` to join rewrite reaches the same conversion through the `EXISTS` it builds.
SELECT 'the IN to join rewrite of a subquery';
SELECT count() FROM t_semi_anti_algo WHERE a NOT IN (SELECT a FROM t_semi_anti_algo WHERE a < 50) SETTINGS rewrite_in_to_join = 1, join_algorithm = 'full_sorting_merge';
SELECT count() FROM t_semi_anti_algo WHERE a NOT IN (SELECT a FROM t_semi_anti_algo WHERE a < 50) SETTINGS rewrite_in_to_join = 1, join_algorithm = 'partial_merge';
SELECT count() FROM t_semi_anti_algo WHERE a IN (SELECT a FROM t_semi_anti_algo WHERE a < 50) SETTINGS rewrite_in_to_join = 1, join_algorithm = 'full_sorting_merge';
SELECT count() FROM t_semi_anti_algo WHERE a NOT IN (SELECT a FROM t_semi_anti_algo WHERE a < 50) SETTINGS rewrite_in_to_join = 1, join_algorithm = 'hash';

-- With a hash algorithm enabled the conversion still happens; with a sort-merge one the join keeps
-- the strictness that algorithm can execute.
SELECT 'the strictness the plan ends up with';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, description = 1
    SELECT count() FROM t_semi_anti_algo AS o WHERE EXISTS (SELECT 1 FROM t_semi_anti_algo AS i WHERE i.b = o.b AND i.a = 5)
    SETTINGS join_algorithm = 'hash', query_plan_convert_any_join_to_semi_or_anti_join = 1) WHERE explain LIKE '%Strictness%';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, description = 1
    SELECT count() FROM t_semi_anti_algo AS o WHERE EXISTS (SELECT 1 FROM t_semi_anti_algo AS i WHERE i.b = o.b AND i.a = 5)
    SETTINGS join_algorithm = 'full_sorting_merge', query_plan_convert_any_join_to_semi_or_anti_join = 1) WHERE explain LIKE '%Strictness%';

DROP TABLE t_semi_anti_algo;
