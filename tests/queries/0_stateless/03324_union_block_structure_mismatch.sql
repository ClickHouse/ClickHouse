SET optimize_use_projections = 1;

-- Reading over parallel replicas both makes normal projections unsupported (unless
-- `parallel_replicas_local_plan` is on and aggregation in order is off, and the runner randomizes
-- both) and wraps the plan in a second `Union` over `ReadFromRemoteParallelReplicas`, which the
-- `Union` count at the end would see. Keep the read local.
SET enable_parallel_replicas = 0;

-- Test for "Block structure mismatch in UnionStep" bug
-- When projection optimization creates a Union between projection and non-projection reads,
-- the branches may have different headers (e.g., due to different query DAGs being applied).
-- Without the fix, this would cause an assertion failure / crash in debug builds.

DROP TABLE IF EXISTS t0;

-- The fixture relies on a mixed-parts state: the first part is written before `ADD PROJECTION`
-- (so it has no projection), the second one after (so it has it). Disable merges so the two parts
-- can never be merged into a single part with the projection materialized, which would silently
-- collapse the mixed `Union` plan below into the all-parts rewrite.
CREATE TABLE t0 (i Int32) ENGINE = MergeTree() ORDER BY tuple() SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO t0 SELECT number FROM numbers(1);
ALTER TABLE t0 ADD PROJECTION x (SELECT i ORDER BY i) SETTINGS mutations_sync = 2;
INSERT INTO t0 SELECT number FROM numbers(1);

-- Pin the mixed-parts state itself: two active parts, exactly one of which carries the projection.
-- If `ADD PROJECTION` ever starts materializing into existing parts, this fails instead of the
-- coverage silently degrading.
SELECT (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't0' AND active) = 2
   AND (SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't0' AND active AND name = 'x') = 1;

-- The mismatch here is a surplus pass-through column, which is narrowed away, so the Union is built
-- from equal headers and the projection is used. `query_plan_remove_unused_columns = 0` is what keeps
-- that surplus column alive; under the default 1 a later pass removes it and the projection is used
-- either way.
SELECT 1 FROM t0 WHERE materialize(1) SETTINGS force_optimize_projection = 1, query_plan_remove_unused_columns = 0;

-- The statement above also passes when the projection is not used at all, so pin the plan it must
-- take: a mixed `Union` whose projection arm reads `x`, whose other arm is a surviving base-table
-- `ReadFromMergeTree` over the projection-less part, and the structure check below the narrowing
-- does not decline it.
SELECT countIf(explain ILIKE '%Union%') = 1
   AND countIf(explain ILIKE '%ReadFromMergeTree (x)%') = 1
   AND countIf(explain ILIKE '%ReadFromMergeTree%' AND explain NOT ILIKE '%ReadFromMergeTree (x)%') = 1
   AND countIf(explain ILIKE '%does not match the structure it replaces%') = 0
FROM (EXPLAIN projections = 1 SELECT 1 FROM t0 WHERE materialize(1) SETTINGS force_optimize_projection = 1, query_plan_remove_unused_columns = 0);

DROP TABLE t0;
