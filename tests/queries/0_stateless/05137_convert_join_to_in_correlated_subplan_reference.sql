-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/118733.
-- A CommonSubplanReferenceStep holds a raw pointer to the node that must still hold the
-- CommonSubplanStep when the second pass resolves it; the two arms cover the two consumers that do.

SET enable_analyzer = 1;                             -- correlated subqueries need the analyzer
SET allow_experimental_correlated_subqueries = 1;    -- gates decorrelation
SET query_plan_convert_join_to_in = 1;               -- the trigger, default 0
SET query_plan_convert_outer_join_to_inner_join = 1; -- supplies the INNER kind the rewrite requires, and the test runner randomizes it off in 5% of runs
SET join_algorithm = 'hash';                         -- convertJoinToIn only runs for hash/parallel_hash, and the buffer=0 arm keeps the session's list

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
