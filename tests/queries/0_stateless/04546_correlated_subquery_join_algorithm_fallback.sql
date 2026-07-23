-- A correlated subquery (EXISTS / NOT EXISTS, or IN / NOT IN under rewrite_in_to_join) decorrelates
-- into an internal ANY join whose kind is correlated_subqueries_default_join_kind (RIGHT by default),
-- and which may then be turned into SEMI / ANTI by the convert-any-join-to-semi-or-anti optimization.
-- full_sorting_merge, partial_merge and direct cannot execute some of those combinations and have no
-- fallback, so the whole query used to fail with NOT_IMPLEMENTED. Two coordinated fixes keep it runnable:
--   * the internal ANY join gets a hash fallback only when no enabled algorithm can run its ANY shape
--     (adding hash unconditionally would let the runtime-filter pass drop a runnable full_sorting_merge
--     plan and demote it to in-memory hash), and
--   * the convert-any-join-to-semi-or-anti pass only rewrites to SEMI/ANTI when an enabled algorithm can
--     execute the result, otherwise it keeps the runnable ANY join.
-- Regression for #111207 and #111075.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS t_04546;
CREATE TABLE t_04546 (a UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_04546 SELECT number FROM numbers(100);

-- rewrite_in_to_join: IN / NOT IN with sort-merge algorithms (the reported cases). All must return 50.
SELECT count() FROM t_04546 WHERE a IN     (SELECT a FROM t_04546 WHERE a < 50) SETTINGS rewrite_in_to_join = 1, join_algorithm = 'full_sorting_merge';
SELECT count() FROM t_04546 WHERE a NOT IN (SELECT a FROM t_04546 WHERE a < 50) SETTINGS rewrite_in_to_join = 1, join_algorithm = 'full_sorting_merge';
SELECT count() FROM t_04546 WHERE a IN     (SELECT a FROM t_04546 WHERE a < 50) SETTINGS rewrite_in_to_join = 1, join_algorithm = 'partial_merge';
SELECT count() FROM t_04546 WHERE a NOT IN (SELECT a FROM t_04546 WHERE a < 50) SETTINGS rewrite_in_to_join = 1, join_algorithm = 'partial_merge';

-- Same limitation reachable without rewrite_in_to_join, via a plain correlated EXISTS / NOT EXISTS.
SELECT count() FROM t_04546 AS o WHERE     EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'partial_merge';
SELECT count() FROM t_04546 AS o WHERE NOT EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'partial_merge';

-- The other decorrelation kind: NOT EXISTS becomes an internal ANTI join, which partial_merge cannot execute.
SELECT count() FROM t_04546 AS o WHERE NOT EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'partial_merge', correlated_subqueries_default_join_kind = 'left';
SELECT count() FROM t_04546 WHERE a NOT IN (SELECT a FROM t_04546 WHERE a < 50) SETTINGS rewrite_in_to_join = 1, join_algorithm = 'partial_merge', correlated_subqueries_default_join_kind = 'left';

-- The in-memory-buffer path (default) restricts the internal join's algorithms to the hash family; the
-- fallback must still leave it runnable. Disable equivalent-expression substitution so the correlated
-- input subplan stays referenced and this buffer-specific algorithm filtering is actually exercised.
SELECT count() FROM t_04546 AS o WHERE EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'partial_merge', correlated_subqueries_use_in_memory_buffer = 1, correlated_subqueries_substitute_equivalent_expressions = 0;

-- direct has no fallback of its own either.
SELECT count() FROM t_04546 AS o WHERE EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'direct';

-- Control: hash always worked.
SELECT count() FROM t_04546 WHERE a NOT IN (SELECT a FROM t_04546 WHERE a < 50) SETTINGS rewrite_in_to_join = 1, join_algorithm = 'hash';

-- full_sorting_merge CAN execute the ANY join but not SEMI/ANTI. The convert pass must keep the ANY join
-- (returns the result) instead of rewriting to an unrunnable SEMI/ANTI. Both convert on and off must agree.
SELECT count() FROM t_04546 AS o WHERE     EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'full_sorting_merge', query_plan_convert_any_join_to_semi_or_anti_join = 1;
SELECT count() FROM t_04546 AS o WHERE NOT EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'full_sorting_merge', query_plan_convert_any_join_to_semi_or_anti_join = 1;
SELECT count() FROM t_04546 AS o WHERE     EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'full_sorting_merge', query_plan_convert_any_join_to_semi_or_anti_join = 0;
SELECT count() FROM t_04546 AS o WHERE NOT EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50) SETTINGS join_algorithm = 'full_sorting_merge', query_plan_convert_any_join_to_semi_or_anti_join = 0;

-- When full_sorting_merge is combined with hash, hash can execute the converted SEMI/ANTI, so the pass
-- still converts and results are unchanged. Result counts alone cannot tell conversion from a declined
-- rewrite (both return 50) and the convert setting is randomized in CI, so assert the plan directly with
-- the rewrite pinned on: EXISTS becomes SEMI, NOT EXISTS becomes ANTI.
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

-- Plan assertions: with full_sorting_merge and the runtime-filter pass enabled (default), the internal
-- decorrelation join must stay a full sorting merge join. Result counts alone cannot catch this: an
-- unconditional hash fallback keeps the result 50 while the runtime-filter pass silently drops
-- full_sorting_merge and demotes the spill-capable plan to an in-memory hash build. Assert the merge
-- join is present (JOIN YShaped) and no hash join was substituted (JOIN FillRightFirst absent).
SELECT count() > 0 FROM (
    EXPLAIN SELECT count() FROM t_04546 AS o WHERE EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50)
    SETTINGS join_algorithm = 'full_sorting_merge', query_plan_convert_any_join_to_semi_or_anti_join = 0, enable_join_runtime_filters = 1
) WHERE explain ILIKE '%JOIN YShaped%';
SELECT count() FROM (
    EXPLAIN SELECT count() FROM t_04546 AS o WHERE EXISTS (SELECT 1 FROM t_04546 AS i WHERE i.a = o.a AND i.a < 50)
    SETTINGS join_algorithm = 'full_sorting_merge', query_plan_convert_any_join_to_semi_or_anti_join = 0, enable_join_runtime_filters = 1
) WHERE explain ILIKE '%FillRightFirst%';

DROP TABLE t_04546;

-- The capability guard must reflect the algorithm that can actually run THIS join. direct only
-- applies to a key-value right side; for an ordinary right side (here numbers()) it cannot run, so
-- with join_algorithm = 'full_sorting_merge,direct' the ANY join is runnable only as full_sorting_merge
-- (no SEMI/ANTI). Counting direct as SEMI-capable regardless of the right side would convert the join
-- and leave neither algorithm able to execute it (NOT_IMPLEMENTED). It must stay runnable.
SELECT count() FROM numbers(3) AS l LEFT ANY JOIN numbers(2) AS r ON l.number = r.number
WHERE r.number != 0
SETTINGS join_algorithm = 'full_sorting_merge,direct', query_plan_convert_any_join_to_semi_or_anti_join = 1;
SELECT count() FROM numbers(3) AS l LEFT ANY JOIN numbers(2) AS r ON l.number = r.number
WHERE r.number != 0
SETTINGS join_algorithm = 'full_sorting_merge,direct', query_plan_convert_any_join_to_semi_or_anti_join = 0;

-- A Join-engine table reuses its stored join and requires its declared ANY strictness unchanged, so
-- the convert pass must skip it: rewriting to SEMI would make physical planning reject the stored join
-- (INCOMPATIBLE_TYPE_OF_JOIN). The query must stay runnable with the rewrite enabled.
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
