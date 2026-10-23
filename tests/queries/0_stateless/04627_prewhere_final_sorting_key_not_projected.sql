-- Regression test for issue #111709: a FINAL query filtering on a sorting-key column that is
-- not in the SELECT list must not throw NOT_FOUND_COLUMN_IN_BLOCK when the filter is in PREWHERE.

SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1;
SET optimize_move_to_prewhere_if_final = 1;
-- Pin both prewhere-related optimizations (the runner disables each with 5% probability):
-- query_plan_optimize_prewhere keeps the EXPLAIN assertion stable, and query_plan_remove_unused_columns
-- must stay on or the pruning path this fix touches is skipped and the test stops guarding the fix.
SET query_plan_optimize_prewhere = 1;
SET query_plan_remove_unused_columns = 1;

DROP TABLE IF EXISTS t_04627_summing;
CREATE TABLE t_04627_summing (k UInt32, s Int64)
ENGINE = SummingMergeTree ORDER BY k SETTINGS optimize_on_insert = 0;
SYSTEM STOP MERGES t_04627_summing;
INSERT INTO t_04627_summing SELECT number % 10 + 1, 3 FROM numbers(50);
INSERT INTO t_04627_summing SELECT number % 10 + 1, 3 FROM numbers(50);

-- The failing query from the issue: k is filtered but not projected. Must return sum(s) = 300.
SELECT sum(s) FROM t_04627_summing FINAL WHERE k GROUP BY s;
-- Must match the result with the optimization disabled (returns 1).
SELECT (SELECT sum(s) FROM t_04627_summing FINAL WHERE k GROUP BY s)
     = (SELECT sum(s) FROM t_04627_summing FINAL WHERE k GROUP BY s SETTINGS optimize_move_to_prewhere_if_final = 0);
-- Explicit PREWHERE on the unprojected key hits the same merge-key pruning path.
SELECT sum(s) FROM t_04627_summing FINAL PREWHERE k GROUP BY s;
-- The old planner (enable_analyzer = 0) shares the pruning path too.
-- The optimization must still fire: the filter on k is moved to PREWHERE (returns 1).
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT s FROM t_04627_summing FINAL WHERE k GROUP BY s)
WHERE explain ILIKE '%Prewhere filter column:%k%';

DROP TABLE t_04627_summing;

-- Composite sorting key: filter on the second key column (not projected) must keep both a and b
-- readable for the merge. Returns 1 (FINAL result matches the optimization-disabled result).
DROP TABLE IF EXISTS t_04627_composite;
CREATE TABLE t_04627_composite (a UInt32, b UInt32, val Int64)
ENGINE = SummingMergeTree ORDER BY (a, b) SETTINGS optimize_on_insert = 0;
SYSTEM STOP MERGES t_04627_composite;
INSERT INTO t_04627_composite SELECT number % 10, number % 7 + 1, 5 FROM numbers(100);
INSERT INTO t_04627_composite SELECT number % 10, number % 7 + 1, 5 FROM numbers(100);
SELECT (SELECT groupArray(c) FROM (SELECT sum(val) AS c FROM t_04627_composite FINAL WHERE b GROUP BY val ORDER BY val))
     = (SELECT groupArray(c) FROM (SELECT sum(val) AS c FROM t_04627_composite FINAL WHERE b GROUP BY val ORDER BY val SETTINGS optimize_move_to_prewhere_if_final = 0));
DROP TABLE t_04627_composite;

-- CoalescingMergeTree shares the same FINAL merge path.
DROP TABLE IF EXISTS t_04627_coalescing;
CREATE TABLE t_04627_coalescing (k UInt32, s Int64)
ENGINE = CoalescingMergeTree ORDER BY k SETTINGS optimize_on_insert = 0;
SYSTEM STOP MERGES t_04627_coalescing;
INSERT INTO t_04627_coalescing SELECT number % 10 + 1, 3 FROM numbers(50);
INSERT INTO t_04627_coalescing SELECT number % 10 + 1, 3 FROM numbers(50);
SELECT DISTINCT s FROM t_04627_coalescing FINAL WHERE k GROUP BY s;
DROP TABLE t_04627_coalescing;

-- ReplacingMergeTree shares the same FINAL merge path.
DROP TABLE IF EXISTS t_04627_replacing;
CREATE TABLE t_04627_replacing (k UInt32, v UInt32, s Int64)
ENGINE = ReplacingMergeTree(v) ORDER BY k SETTINGS optimize_on_insert = 0;
SYSTEM STOP MERGES t_04627_replacing;
INSERT INTO t_04627_replacing SELECT number % 10 + 1, 1, 3 FROM numbers(50);
INSERT INTO t_04627_replacing SELECT number % 10 + 1, 2, 7 FROM numbers(50);
SELECT DISTINCT s FROM t_04627_replacing FINAL WHERE k GROUP BY s;
DROP TABLE t_04627_replacing;

-- AggregatingMergeTree (SimpleAggregateFunction) shares the same FINAL merge path.
DROP TABLE IF EXISTS t_04627_aggregating;
CREATE TABLE t_04627_aggregating (k UInt32, s SimpleAggregateFunction(sum, Int64))
ENGINE = AggregatingMergeTree ORDER BY k SETTINGS optimize_on_insert = 0;
SYSTEM STOP MERGES t_04627_aggregating;
INSERT INTO t_04627_aggregating SELECT number % 10 + 1, 3 FROM numbers(50);
INSERT INTO t_04627_aggregating SELECT number % 10 + 1, 3 FROM numbers(50);
SELECT sum(s) FROM t_04627_aggregating FINAL WHERE k GROUP BY s;
DROP TABLE t_04627_aggregating;
