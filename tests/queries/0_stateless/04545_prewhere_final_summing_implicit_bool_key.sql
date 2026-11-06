-- Regression test for issue #111312: optimize_move_to_prewhere_if_final returned wrong
-- results on SummingMergeTree FINAL when an implicit-boolean WHERE on the sorting key
-- (e.g. `WHERE k`) was moved to PREWHERE. The analyzer aliases the key column, and moving
-- that alias below FINAL fed a second copy of the key into the merge, which summed it across
-- the unmerged parts (k came back as k * number_of_parts). The optimization must still be
-- applied, only the key must be preserved.

SET enable_analyzer = 1;
SET max_threads = 1;
SET optimize_move_to_prewhere = 1;
SET optimize_move_to_prewhere_if_final = 1;
-- Pin the new-planner prewhere optimization: the runner disables it with 5% probability,
-- which would make the "optimization still fires" EXPLAIN assertion below flaky.
SET query_plan_optimize_prewhere = 1;

DROP TABLE IF EXISTS t_04545_summing;

CREATE TABLE t_04545_summing (k UInt32, s Int64)
ENGINE = SummingMergeTree ORDER BY k SETTINGS optimize_on_insert = 0;

SYSTEM STOP MERGES t_04545_summing;
INSERT INTO t_04545_summing SELECT number % 100, 3 FROM numbers(100);
INSERT INTO t_04545_summing SELECT number % 100, 3 FROM numbers(100);
INSERT INTO t_04545_summing SELECT number % 100, 3 FROM numbers(100);

-- The key k must be preserved (1, 2, 3, ...), not summed across the 3 parts; s must be 9.
SELECT k, s FROM t_04545_summing FINAL WHERE k GROUP BY k, s ORDER BY k, s LIMIT 5;

-- Must match the result with the optimization disabled.
SELECT k, s FROM t_04545_summing FINAL WHERE k GROUP BY k, s ORDER BY k, s LIMIT 5
SETTINGS optimize_move_to_prewhere_if_final = 0;

-- The optimization must still fire: the filter on k is moved to PREWHERE.
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT k, s FROM t_04545_summing FINAL WHERE k GROUP BY k, s)
WHERE explain ILIKE '%Prewhere filter column:%k%';

DROP TABLE t_04545_summing;

-- Composite sorting key: implicit-bool filter on the second key column must not sum a or b.
DROP TABLE IF EXISTS t_04545_composite;

CREATE TABLE t_04545_composite (a UInt32, b UInt32, val Int64)
ENGINE = SummingMergeTree ORDER BY (a, b) SETTINGS optimize_on_insert = 0;

SYSTEM STOP MERGES t_04545_composite;
INSERT INTO t_04545_composite SELECT number % 10, number % 7 + 1, 5 FROM numbers(100);
INSERT INTO t_04545_composite SELECT number % 10, number % 7 + 1, 5 FROM numbers(100);

SELECT a, b, val FROM t_04545_composite FINAL WHERE b GROUP BY a, b, val ORDER BY a, b LIMIT 4;
SELECT a, b, val FROM t_04545_composite FINAL WHERE b GROUP BY a, b, val ORDER BY a, b LIMIT 4
SETTINGS optimize_move_to_prewhere_if_final = 0;

DROP TABLE t_04545_composite;

-- CoalescingMergeTree shares the same FINAL merge path and must be correct as well.
DROP TABLE IF EXISTS t_04545_coalescing;

CREATE TABLE t_04545_coalescing (k UInt32, s Int64)
ENGINE = CoalescingMergeTree ORDER BY k SETTINGS optimize_on_insert = 0;

SYSTEM STOP MERGES t_04545_coalescing;
INSERT INTO t_04545_coalescing SELECT number % 100, 3 FROM numbers(100);
INSERT INTO t_04545_coalescing SELECT number % 100, 3 FROM numbers(100);

SELECT k FROM t_04545_coalescing FINAL WHERE k GROUP BY k, s ORDER BY k LIMIT 5;

DROP TABLE t_04545_coalescing;
