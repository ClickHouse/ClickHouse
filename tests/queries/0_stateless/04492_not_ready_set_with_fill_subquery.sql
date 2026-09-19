-- Regression test for "Not-ready Set is passed as the second argument" when the `IN` subquery
-- source plan contains a step that used to be non-clonable, so `buildOrderedSetInplace` took the
-- destructive fallback (consuming `source`) and could not recover from a silent in-place failure.
--
-- This complements `04095_global_not_in_parallel_replicas`, which only covers a plain
-- `ReadFromMergeTree` + filter subquery plan. Here the subquery has `ORDER BY ... WITH FILL`, so
-- its source plan is `Filling` -> `Sorting` -> `ReadFromMergeTree`. Before `FillingStep::clone`
-- was implemented, `source->clone()` threw `NOT_IMPLEMENTED`, `buildOrderedSetInplace` consumed
-- the source, and a silent in-place build failure (forced here by the failpoint) left the set
-- permanently unbuilt so `FunctionIn` threw. With `clone` implemented the cloned source is
-- preserved and `DelayedCreatingSetsStep::makePlansForSets` rebuilds the set in the deferred
-- pipeline.
--
-- Tags: no-parallel
-- - no-parallel - global failpoint `prepared_sets_build_ordered_set_inplace_fail`

DROP TABLE IF EXISTS with_fill_in_pr;
CREATE TABLE with_fill_in_pr (dt DateTime, idx Int32) ENGINE = MergeTree() PARTITION BY dt ORDER BY idx;
INSERT INTO with_fill_in_pr SELECT number % 3, number FROM system.numbers LIMIT 99999;

SET use_index_for_in_with_subqueries = 1;

-- `WITH FILL` cannot be serialized in a sort description (`serializeSortDescription` throws
-- `NOT_IMPLEMENTED`). The "distributed plan" CI configuration enables `serialize_query_plan` in the
-- default profile, which would make this subquery's plan serialization fail with an error unrelated
-- to the `Not-ready Set` recovery this test exercises. Keep the plan un-serialized so the test is
-- deterministic across configurations.
SET serialize_query_plan = 0;

-- Sanity check: the query produces the correct result without the failpoint.
-- Rows with dt = number % 3 == 2 have idx in {2, 5, 8, ..., 99998} (min 2, max 99998); `WITH FILL`
-- (default step 1) fills the ordered result to {2, 3, ..., 99998}, so the outer set matches every
-- idx in [2, 99998] = 99997 rows.
SELECT count() == 99997 FROM with_fill_in_pr WHERE idx in (SELECT idx FROM with_fill_in_pr WHERE dt = 2 ORDER BY idx WITH FILL);

-- The failpoint fires ONCE: it skips `finishInsert` on the first `CreatingSetsTransform` pass
-- (the in-place build during primary key analysis), so `buildOrderedSetInplace` returns nullptr.
-- With `FillingStep::clone`, the cloned source plan lets `makePlansForSets` rebuild the set in the
-- deferred pipeline, where the failpoint is already consumed. Without the clone, the source is
-- consumed up front and `FunctionIn` would throw "Not-ready Set is passed as the second argument".
SYSTEM ENABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;

SELECT count() == 99997 FROM with_fill_in_pr WHERE idx in (SELECT idx FROM with_fill_in_pr WHERE dt = 2 ORDER BY idx WITH FILL);

-- Disable the failpoint so it does not leak into other tests in the same server process.
SYSTEM DISABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;

DROP TABLE with_fill_in_pr;
