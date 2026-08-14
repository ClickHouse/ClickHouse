-- Regression test for a LOGICAL_ERROR in `ReadFromMergeTree::removeUnusedColumns`:
-- "Unexpected number of kept output positions after removing unused columns from ReadFromMergeTree".
--
-- Column pruning was accepted when a column that `FINAL` reads for merging (`ver`, `is_deleted`,
-- `sign`) was consumed by `PREWHERE` and therefore absent from the output header. Recomputing the
-- header re-added it, making the header longer than the caller's position vector.
--
-- `OPTIMIZE FINAL` merges all parts into one before each SELECT, so the result of `PREWHERE` on
-- `FINAL` is deterministic regardless of `enable_vertical_final` / `query_plan_optimize_prewhere`.

DROP TABLE IF EXISTS t_replacing_is_deleted_04899;
CREATE TABLE t_replacing_is_deleted_04899
(
    key Int64,
    someCol String,
    ver UInt64,
    is_deleted UInt8
) ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY key;

INSERT INTO t_replacing_is_deleted_04899 VALUES (1, 'test1', 1, 0), (1, 'test2', 2, 0), (2, 'test3', 1, 0), (2, 'test4', 2, 1), (3, 'test5', 1, 1);
OPTIMIZE TABLE t_replacing_is_deleted_04899 FINAL;

-- `query_plan_remove_unused_columns` is randomized off in 5% of CI runs and solely gates the
-- pruning path below, so each query pins it to keep the arm live.
SELECT key, someCol FROM t_replacing_is_deleted_04899 FINAL PREWHERE (ver > 0) OR (database != '') ORDER BY key
SETTINGS query_plan_remove_unused_columns = 1;
SELECT key, someCol FROM merge(currentDatabase(), '^t_replacing_is_deleted_04899$') FINAL PREWHERE (ver > 0) OR (database != '') ORDER BY key
SETTINGS query_plan_remove_unused_columns = 1;

DROP TABLE t_replacing_is_deleted_04899;

DROP TABLE IF EXISTS t_replacing_04899;
CREATE TABLE t_replacing_04899
(
    key Int64,
    someCol String,
    ver UInt64
) ENGINE = ReplacingMergeTree(ver) ORDER BY key;

INSERT INTO t_replacing_04899 VALUES (1, 'test1', 1), (1, 'test2', 2), (2, 'test3', 1);
OPTIMIZE TABLE t_replacing_04899 FINAL;

SELECT key, someCol FROM t_replacing_04899 FINAL PREWHERE (ver > 0) OR (database != '') ORDER BY key
SETTINGS query_plan_remove_unused_columns = 1;

-- Pruning must still happen for the shapes this exclusion does not cover: here `ver` is read for
-- merging and is also in the output header, so `someCol` is consumed by PREWHERE alone and must not
-- be read out of the RFMT step. Asserted on the plan, since the returned rows are the same either way.
SELECT count() = 0 AS someCol_pruned_from_read
FROM (
    EXPLAIN PLAN header = 1
    SELECT count() FROM t_replacing_04899 FINAL PREWHERE someCol != ''
    SETTINGS explain_query_plan_default = 'legacy', query_plan_remove_unused_columns = 1
)
WHERE explain LIKE '%Header: someCol%';

DROP TABLE t_replacing_04899;

DROP TABLE IF EXISTS t_collapsing_04899;
CREATE TABLE t_collapsing_04899
(
    key Int64,
    someCol String,
    sign Int8
) ENGINE = CollapsingMergeTree(sign) ORDER BY key;

INSERT INTO t_collapsing_04899 VALUES (1, 'test1', 1), (1, 'test1', -1), (2, 'test2', 1), (3, 'test3', 1);
OPTIMIZE TABLE t_collapsing_04899 FINAL;

SELECT key, someCol FROM t_collapsing_04899 FINAL PREWHERE (sign = 1) OR (database != '') ORDER BY key
SETTINGS query_plan_remove_unused_columns = 1;

DROP TABLE t_collapsing_04899;

DROP TABLE IF EXISTS t_versioned_collapsing_04899;
CREATE TABLE t_versioned_collapsing_04899
(
    key Int64,
    someCol String,
    sign Int8,
    ver UInt64
) ENGINE = VersionedCollapsingMergeTree(sign, ver) ORDER BY key;

INSERT INTO t_versioned_collapsing_04899 VALUES (1, 'test1', 1, 1), (1, 'test1', -1, 1), (2, 'test2', 1, 2), (3, 'test3', 1, 1);
OPTIMIZE TABLE t_versioned_collapsing_04899 FINAL;

SELECT key, someCol FROM t_versioned_collapsing_04899 FINAL PREWHERE (sign = 1) OR (database != '') ORDER BY key
SETTINGS query_plan_remove_unused_columns = 1;

DROP TABLE t_versioned_collapsing_04899;
