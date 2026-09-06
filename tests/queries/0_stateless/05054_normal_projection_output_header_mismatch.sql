-- Tags: no-parallel
-- - no-parallel - the failpoint below is server-wide and would decline normal projections for
--   concurrently running tests.

-- Pins the fail-close path of the normal projection rewrite: when the rewritten projection stream
-- does not match the structure of the subplan it replaces, the rewrite must be declined, leaving the
-- regular read with every part, instead of being spliced in with mismatching headers.
--
-- A query cannot produce that mismatch: the two header conversions applied before the check
-- (`makeNarrowingDAG` / `makeMaterializingDAG`) refuse only on headers the analyzer never builds for
-- a `MergeTree` read, so the check is the fail-close net for a residual class with no natural
-- carrier. The failpoint below disables both conversions, so the same all-parts rewrite that the
-- baseline below accepts reaches the check with a surplus pass-through column and must be declined.

SET optimize_use_projections = 1;

-- Parallel replicas make normal projections unsupported under some of the randomized settings and
-- wrap the plan in an extra `Union`; keep the read local.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_fail_close;

CREATE TABLE t_fail_close (i Int32) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_fail_close ADD PROJECTION x (SELECT i ORDER BY i) SETTINGS mutations_sync = 2;
INSERT INTO t_fail_close SELECT number FROM numbers(3);

-- The single active part carries the projection, so the rewrite exercised below is the all-parts one
-- (no `Union`). `query_plan_remove_unused_columns = 0` is what keeps the surplus pass-through column
-- that requires the narrowing; under the default 1 a later pass removes it.
SELECT (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_fail_close' AND active) = 1
   AND (SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_fail_close' AND active AND name = 'x') = 1;

-- Baseline: with the conversions in place the whole read is served by the projection and no decline
-- is recorded. This is the same statement that must flip below, so the flip isolates the check.
SELECT countIf(explain ILIKE '%ReadFromMergeTree (x)%') = 1
   AND countIf(explain ILIKE '%ReadFromMergeTree%' AND explain NOT ILIKE '%ReadFromMergeTree (x)%') = 0
   AND countIf(explain ILIKE '%does not match the structure it replaces%') = 0
FROM (EXPLAIN projections = 1 SELECT 1 FROM t_fail_close WHERE materialize(1) SETTINGS force_optimize_projection = 1, query_plan_remove_unused_columns = 0);

SYSTEM ENABLE FAILPOINT normal_projection_skip_output_header_conversions;

-- Declined at the structure check: nothing else changed, so `force_optimize_projection = 1` now finds
-- no projection use. (The rewritten description cannot be read out of `EXPLAIN` here: without
-- `force_optimize_projection` this candidate is dropped earlier by the cost comparison, and with it
-- the decline itself raises before `EXPLAIN` can print the plan.)
SELECT 1 FROM t_fail_close WHERE materialize(1) SETTINGS force_optimize_projection = 1, query_plan_remove_unused_columns = 0; -- { serverError PROJECTION_NOT_USED }

-- The declined rewrite leaves a working full read: every row is still returned, both with the
-- optimization enabled and with it disabled.
SELECT count() FROM t_fail_close WHERE materialize(1) SETTINGS query_plan_remove_unused_columns = 0;
SELECT count() FROM t_fail_close WHERE materialize(1) SETTINGS optimize_use_projections = 0, query_plan_remove_unused_columns = 0;

SYSTEM DISABLE FAILPOINT normal_projection_skip_output_header_conversions;

-- With the conversions back the projection is used again.
SELECT 1 FROM t_fail_close WHERE materialize(1) SETTINGS force_optimize_projection = 1, query_plan_remove_unused_columns = 0;

DROP TABLE t_fail_close;
