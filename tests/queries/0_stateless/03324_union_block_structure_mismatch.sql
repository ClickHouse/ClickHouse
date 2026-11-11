SET optimize_use_projections = 1;

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

-- With force_optimize_projection=1, the projection code path is exercised: the projection read and the table read
-- must be combined by a Union with equal headers, without "Block structure mismatch in UnionStep".
-- Disable unused column removal, so that both reads read `i` only to count rows.
-- `identity` keeps the filter from being folded away.
SELECT 1 FROM t0 WHERE identity(materialize(1)) SETTINGS force_optimize_projection = 1, query_plan_remove_unused_columns = 0;

DROP TABLE t0;
