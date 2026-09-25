-- Tags: no-parallel
-- - no-parallel - global failpoint `prepared_sets_build_ordered_set_inplace_fail`

-- Regression test for "Not-ready Set is passed as the second argument" when the `IN` subquery
-- source plan contains a regular `JoinStep`, exercising `JoinStep::clone` in the non-destructive
-- in-place set build.
--
-- `FutureSetFromSubquery::buildOrderedSetInplace` speculatively builds the set during primary key
-- analysis of an `IN` subquery. This change makes it run against a clone of the subquery `source`
-- plan instead of consuming it, so a silent in-place build failure (forced here once by the
-- failpoint) can still be recovered by the deferred `DelayedCreatingSetsStep::makePlansForSets`.
-- `JoinStep::clone` deep-clones the underlying `IJoin` (a shared `JoinPtr` would let the in-place
-- and the deferred build accumulate join state), so a subquery whose source contains a join needs
-- its own coverage: the complementary tests `04489_not_ready_set_inplace_build` (plain
-- `ReadFromMergeTree`) and `04492_not_ready_set_with_fill_subquery` (`FillingStep`) do not reach it.
--
-- If the join step were not clonable, `source->clone()` would throw `NOT_IMPLEMENTED`,
-- `buildOrderedSetInplace` would consume `source` via the destructive fallback, and the forced
-- in-place failure would leave the set permanently unbuilt so `FunctionIn` would throw. A correct
-- result under the failpoint therefore proves the join source went through `JoinStep::clone`.

DROP TABLE IF EXISTS t_outer;
DROP TABLE IF EXISTS t_small;
DROP TABLE IF EXISTS t_big;

CREATE TABLE t_outer (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_outer SELECT number FROM numbers(1000);

CREATE TABLE t_small (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_small SELECT number FROM numbers(10);
CREATE TABLE t_big (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_big SELECT number FROM numbers(1000);

SET use_index_for_in_with_subqueries = 1;
-- The non-destructive clone path for join sources exists on the analyzer plan (`JoinStepLogical`
-- converted to a physical `JoinStep`); force it so the coverage is deterministic regardless of the
-- old-analyzer CI job.
SET enable_analyzer = 1;

-- The `IN` subquery is an INNER JOIN of `t_small` and `t_big` on `k`, matching the 10 values in
-- [0, 10), so the outer count is 10. `query_plan_join_swap_table` selects which side is the build
-- (right) table, so the `IJoin` `JoinStep::clone` rebuilds is constructed with each header order:
-- 'false' keeps the right table (`t_big`) as the build side, 'true' swaps so the smaller `t_small`
-- becomes the build side.

-- Build side = t_big (no swap).
SET query_plan_join_swap_table = 'false';
SELECT count() FROM t_outer WHERE k IN (SELECT s.k FROM t_small AS s INNER JOIN t_big AS b ON s.k = b.k);

-- The failpoint fires ONCE: it skips `finishInsert` on the first `CreatingSetsTransform` pass (the
-- in-place build during primary key analysis), so `buildOrderedSetInplace` returns nullptr. With
-- `JoinStep::clone` the cloned source plan lets `makePlansForSets` rebuild the set in the deferred
-- pipeline, where the failpoint is already consumed.
SYSTEM ENABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;
SELECT count() FROM t_outer WHERE k IN (SELECT s.k FROM t_small AS s INNER JOIN t_big AS b ON s.k = b.k);
SYSTEM DISABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;

-- Build side = t_small (swapped): the join is cloned with the reversed header order.
SET query_plan_join_swap_table = 'true';
SELECT count() FROM t_outer WHERE k IN (SELECT s.k FROM t_small AS s INNER JOIN t_big AS b ON s.k = b.k);

SYSTEM ENABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;
SELECT count() FROM t_outer WHERE k IN (SELECT s.k FROM t_small AS s INNER JOIN t_big AS b ON s.k = b.k);
SYSTEM DISABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;

DROP TABLE t_outer;
DROP TABLE t_small;
DROP TABLE t_big;
