-- Decorrelated subqueries stay runnable under any join_algorithm. #111207, #111075.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS t_04546;
CREATE TABLE t_04546 (a UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_04546 SELECT number FROM numbers(100);

-- Reported cases.
SELECT count() FROM t_04546 WHERE a IN     (SELECT a FROM t_04546 WHERE a < 50) SETTINGS rewrite_in_to_join = 1, join_algorithm = 'full_sorting_merge';
SELECT count() FROM t_04546 WHERE a NOT IN (SELECT a FROM t_04546 WHERE a < 50) SETTINGS rewrite_in_to_join = 1, join_algorithm = 'full_sorting_merge';
SELECT count() FROM t_04546 WHERE a IN     (SELECT a FROM t_04546 WHERE a < 50) SETTINGS rewrite_in_to_join = 1, join_algorithm = 'partial_merge';
SELECT count() FROM t_04546 WHERE a NOT IN (SELECT a FROM t_04546 WHERE a < 50) SETTINGS rewrite_in_to_join = 1, join_algorithm = 'partial_merge';

-- Same via a plain EXISTS.
SELECT count() FROM t_04546 AS o WHERE     EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'partial_merge';
SELECT count() FROM t_04546 AS o WHERE NOT EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'partial_merge';

-- LEFT kind: ANTI, which partial_merge lacks.
SELECT count() FROM t_04546 AS o WHERE NOT EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'partial_merge', correlated_subqueries_default_join_kind = 'left';
SELECT count() FROM t_04546 WHERE a NOT IN (SELECT a FROM t_04546 WHERE a < 50) SETTINGS rewrite_in_to_join = 1, join_algorithm = 'partial_merge', correlated_subqueries_default_join_kind = 'left';

-- Buffer path: hash only. Substitution off, else not reached.
SELECT count() FROM t_04546 AS o WHERE EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'partial_merge', correlated_subqueries_use_in_memory_buffer = 1, correlated_subqueries_substitute_equivalent_expressions = 0;

-- direct has no fallback.
SELECT count() FROM t_04546 AS o WHERE EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'direct';

-- Control.
SELECT count() FROM t_04546 WHERE a NOT IN (SELECT a FROM t_04546 WHERE a < 50) SETTINGS rewrite_in_to_join = 1, join_algorithm = 'hash';

-- full_sorting_merge runs ANY, not SEMI/ANTI.
SELECT count() FROM t_04546 AS o WHERE     EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'full_sorting_merge', query_plan_convert_any_join_to_semi_or_anti_join = 1;
SELECT count() FROM t_04546 AS o WHERE NOT EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'full_sorting_merge', query_plan_convert_any_join_to_semi_or_anti_join = 1;
SELECT count() FROM t_04546 AS o WHERE     EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'full_sorting_merge', query_plan_convert_any_join_to_semi_or_anti_join = 0;
SELECT count() FROM t_04546 AS o WHERE NOT EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'full_sorting_merge', query_plan_convert_any_join_to_semi_or_anti_join = 0;

-- With hash it still converts; both return 50, so assert the plan.
SELECT count() FROM t_04546 AS o WHERE     EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'full_sorting_merge,hash';
SELECT count() FROM t_04546 AS o WHERE NOT EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'full_sorting_merge,hash';
SELECT count() > 0 FROM (
    EXPLAIN SELECT count() FROM t_04546 AS o WHERE EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50)
    SETTINGS join_algorithm = 'full_sorting_merge,hash', query_plan_convert_any_join_to_semi_or_anti_join = 1
) WHERE explain ILIKE '%Strictness: semi%';
SELECT count() > 0 FROM (
    EXPLAIN SELECT count() FROM t_04546 AS o WHERE NOT EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50)
    SETTINGS join_algorithm = 'full_sorting_merge,hash', query_plan_convert_any_join_to_semi_or_anti_join = 1
) WHERE explain ILIKE '%Strictness: anti%';

-- Must stay a merge join: a blanket hash fallback returns 50 but demotes it.
SELECT count() > 0 FROM (
    EXPLAIN SELECT count() FROM t_04546 AS o WHERE EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50)
    SETTINGS join_algorithm = 'full_sorting_merge', query_plan_convert_any_join_to_semi_or_anti_join = 0, enable_join_runtime_filters = 1
) WHERE explain ILIKE '%JOIN YShaped%';
SELECT count() FROM (
    EXPLAIN SELECT count() FROM t_04546 AS o WHERE EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50)
    SETTINGS join_algorithm = 'full_sorting_merge', query_plan_convert_any_join_to_semi_or_anti_join = 0, enable_join_runtime_filters = 1
) WHERE explain ILIKE '%FillRightFirst%';

DROP TABLE t_04546;

-- direct cannot run an ordinary right side; calling it SEMI-capable leaves nothing that can.
SELECT count() FROM numbers(3) AS l LEFT ANY JOIN numbers(2) AS r ON l.number = r.number
WHERE r.number != 0
SETTINGS join_algorithm = 'full_sorting_merge,direct', query_plan_convert_any_join_to_semi_or_anti_join = 1;
SELECT count() FROM numbers(3) AS l LEFT ANY JOIN numbers(2) AS r ON l.number = r.number
WHERE r.number != 0
SETTINGS join_algorithm = 'full_sorting_merge,direct', query_plan_convert_any_join_to_semi_or_anti_join = 0;

-- A Join-engine right side rejects a changed strictness.
DROP TABLE IF EXISTS t_04546_left;
DROP TABLE IF EXISTS join_04546;
CREATE TABLE t_04546_left (id UInt64) ENGINE = Memory;
INSERT INTO t_04546_left SELECT number FROM numbers(3);
CREATE TABLE join_04546 (id UInt64, val String) ENGINE = Join(ANY, LEFT, id);
INSERT INTO join_04546 VALUES (0, 'zero'), (1, 'one');
SELECT count() FROM t_04546_left AS l LEFT ANY JOIN join_04546 AS r USING (id) WHERE r.val != ''
SETTINGS query_plan_convert_any_join_to_semi_or_anti_join = 1;
SELECT count() FROM t_04546_left AS l LEFT ANY JOIN join_04546 AS r USING (id) WHERE r.val != ''
SETTINGS query_plan_convert_any_join_to_semi_or_anti_join = 0;

DROP TABLE t_04546_left;
DROP TABLE join_04546;
