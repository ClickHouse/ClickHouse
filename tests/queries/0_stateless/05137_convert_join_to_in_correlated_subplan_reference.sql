-- Regression test for issue #118733.
-- Decorrelating a correlated scalar subquery puts a CommonSubplanStep on the outer plan's root and
-- gives the decorrelated subquery a CommonSubplanReferenceStep holding a raw pointer to that node.
-- Both become inputs of one JoinStepLogical, and the `query_plan_convert_join_to_in` rewrite
-- installed new steps at the input node addresses and spliced the right input into the IN set's own
-- plan, so the reference no longer resolved to a CommonSubplanStep:
--   Logical error: Expected CommonSubplanReferenceStep to reference CommonSubplanStep, but got Expression
-- thrown from useMemoryBufferForCommonSubplanResult with the in-memory buffer enabled, and from
-- materializeQueryPlanReferences with it disabled.

SET enable_analyzer = 1;                             -- correlated subqueries need the analyzer
SET allow_experimental_correlated_subqueries = 1;    -- gates decorrelation
SET query_plan_convert_join_to_in = 1;               -- the trigger, default 0
SET query_plan_convert_outer_join_to_inner_join = 1; -- supplies the INNER kind the rewrite requires, and the test runner randomizes it off in 5% of runs

DROP TABLE IF EXISTS t_05137;

CREATE TABLE t_05137 (a UInt32, b Nullable(Int64)) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_05137 VALUES (1, 1), (2, NULL), (3, 5), (4, 5), (5, 5);

-- `max(b)` is correlated, so it is the row's own `b`: the first count is 0, and the second counts
-- every row with a non-NULL `b`. Duplicate `b` values make the second count sensitive to a rewrite
-- that collapsed the outer stream to its distinct correlated keys.
SELECT '-- in-memory buffer';
SELECT count() FROM t_05137 WHERE 1 > (SELECT max(b)) SETTINGS correlated_subqueries_use_in_memory_buffer = 1;
SELECT count() FROM t_05137 WHERE 100 > (SELECT max(b)) SETTINGS correlated_subqueries_use_in_memory_buffer = 1;

SELECT '-- no in-memory buffer';
SELECT count() FROM t_05137 WHERE 1 > (SELECT max(b)) SETTINGS correlated_subqueries_use_in_memory_buffer = 0;
SELECT count() FROM t_05137 WHERE 100 > (SELECT max(b)) SETTINGS correlated_subqueries_use_in_memory_buffer = 0;

DROP TABLE t_05137;
