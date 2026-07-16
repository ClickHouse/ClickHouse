-- Tags: no-parallel
-- no-parallel: uses the global ONCE failpoint `prepared_sets_build_ordered_set_inplace_fail`.

-- Regression test for "Not-ready Set is passed as the second argument".
--
-- `FutureSetFromSubquery::buildOrderedSetInplace` is a speculative build run during primary key
-- analysis for an `IN` subquery. It used to consume the subquery `source` plan up front. If the
-- in-place pipeline then stopped without creating the set (e.g. a subquery timeout with
-- `overflow_mode = 'break'`, or any early stop that skips `Set::finishInsert`), the consumed
-- `source` left the set permanently unbuilt, and `FunctionIn` threw when the main pipeline ran.
--
-- The fix runs the in-place pipeline against a clone of `source`, leaving the original intact so
-- `DelayedCreatingSetsStep::makePlansForSets` can still build the set. The failpoint
-- `prepared_sets_build_ordered_set_inplace_fail` fires once inside `CreatingSetsTransform::generate`
-- and skips `finishInsert`, reproducing the silent in-place failure deterministically.

DROP TABLE IF EXISTS t_not_ready_set;
CREATE TABLE t_not_ready_set (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_not_ready_set SELECT number FROM numbers(1000);

SET use_index_for_in_with_subqueries = 1;

-- Sanity check: correct result without the failpoint.
SELECT count() FROM t_not_ready_set WHERE k IN (SELECT k FROM t_not_ready_set WHERE k < 500);

-- `k` is the primary key, so the `IN` subquery triggers primary key analysis and
-- `buildOrderedSetInplace` is called here. The failpoint makes that in-place build skip
-- `finishInsert` once, so the set is not created on the in-place pass. With the fix the cloned
-- source preserves the original `source` plan and the deferred build creates the set; without the
-- fix `FunctionIn` would throw "Not-ready Set is passed as the second argument".
SYSTEM ENABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;
SELECT count() FROM t_not_ready_set WHERE k IN (SELECT k FROM t_not_ready_set WHERE k < 500);
SYSTEM DISABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;

-- Nested `IN`: the inner subquery's source plan itself contains a set-building step, so the outer
-- subquery `source` is a `DelayedCreatingSetsStep`, which is intentionally left non-clonable; that
-- subquery shape takes the destructive fallback (as it always did before this change). The whole
-- query must still produce the correct result when the in-place build fails silently once: the
-- `ONCE` failpoint fires on the first (innermost, clonable) set, whose deferred build recovers it.
SELECT count() FROM t_not_ready_set
WHERE k IN (SELECT k FROM t_not_ready_set WHERE k IN (SELECT k FROM t_not_ready_set WHERE k < 250));

SYSTEM ENABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;
SELECT count() FROM t_not_ready_set
WHERE k IN (SELECT k FROM t_not_ready_set WHERE k IN (SELECT k FROM t_not_ready_set WHERE k < 250));
SYSTEM DISABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;

DROP TABLE t_not_ready_set;
