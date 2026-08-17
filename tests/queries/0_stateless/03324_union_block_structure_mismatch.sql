SET optimize_use_projections = 1;

-- Test for "Block structure mismatch in UnionStep" bug
-- When projection optimization creates a Union between projection and non-projection reads,
-- the branches may have different headers (e.g., due to different query DAGs being applied).
-- Without the fix, this would cause an assertion failure / crash in debug builds.

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (i Int32) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO t0 SELECT number FROM numbers(1);
ALTER TABLE t0 ADD PROJECTION x (SELECT i ORDER BY i) SETTINGS mutations_sync = 2;
INSERT INTO t0 SELECT number FROM numbers(1);

-- The mismatch here is a surplus pass-through column, which is narrowed away, so the Union is built
-- from equal headers and the projection is used. `query_plan_remove_unused_columns = 0` is what keeps
-- that surplus column alive; under the default 1 a later pass removes it and the projection is used
-- either way.
SELECT 1 FROM t0 WHERE materialize(1) SETTINGS force_optimize_projection = 1, query_plan_remove_unused_columns = 0;

-- The statement above also passes when the projection is not used at all, so pin the plan it must
-- take: the projection is read, and the structure check below the narrowing does not decline it.
SELECT countIf(explain ILIKE '%ReadFromMergeTree (x)%') = 1 AND countIf(explain ILIKE '%does not match the structure it replaces%') = 0
FROM (EXPLAIN projections = 1 SELECT 1 FROM t0 WHERE materialize(1) SETTINGS force_optimize_projection = 1, query_plan_remove_unused_columns = 0);

DROP TABLE t0;
