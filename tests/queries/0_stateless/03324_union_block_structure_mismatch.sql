-- Test for "Block structure mismatch in UnionStep" bug
-- When projection optimization creates a Union between projection and non-projection reads,
-- the branches may have different headers (e.g., due to different query DAGs being applied).
-- Without the fix, this would cause an assertion failure / crash in debug builds.
-- With the fix, the projection optimization is safely skipped when headers don't match.

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (i Int32) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO t0 SELECT number FROM numbers(1);
ALTER TABLE t0 ADD PROJECTION x (SELECT i ORDER BY i) SETTINGS mutations_sync = 2;
INSERT INTO t0 SELECT number FROM numbers(1);

-- With force_optimize_projection=1, the projection code path is exercised.
-- The fix causes it to safely skip the optimization and return PROJECTION_NOT_USED error
-- instead of crashing with "Block structure mismatch in UnionStep".
-- Disable unused column removal as it makes using the projection possible, because it can remove
-- all columns, making the headers empty.
SELECT 1 FROM t0 WHERE materialize(1) SETTINGS force_optimize_projection = 1, query_plan_remove_unused_columns = 0; -- { serverError PROJECTION_NOT_USED }

DROP TABLE t0;
