-- Tags: no-old-analyzer
--
-- Regression test for a LOGICAL_ERROR in `ReadFromMergeTree::removeUnusedColumns`:
-- "Unexpected number of kept output positions after removing unused columns from ReadFromMergeTree".
--
-- Bare `database` below is the parenthesis-less `currentDatabase()` alias, which only the analyzer
-- resolves; the old analyzer reads it as a column and fails with UNKNOWN_IDENTIFIER. It is load
-- bearing and cannot be spelled `currentDatabase()`: that form folds the whole `PREWHERE` to a
-- constant before the read set is computed, so `ver` is never pruned and the bug is not reached.
--
-- A column that `FINAL` reads for merging (`ver`, `is_deleted`, `sign`) but that `PREWHERE` had
-- consumed was added back to the read set, so the recomputed step header was longer than the
-- caller's position vector.
--
-- `query_plan_remove_unused_columns` is randomized off in 5% of CI runs and solely gates the
-- pruning path, so every query below pins it. Every fixture stops merges and asserts it still has
-- more than one active part: once collapsed, a deduplication oracle is satisfied by data that never
-- needed a merge, so an arm that stopped reading the merging column would still look correct.

DROP TABLE IF EXISTS t_replacing_is_deleted_04906;
CREATE TABLE t_replacing_is_deleted_04906
(
    key Int64,
    someCol String,
    ver UInt64,
    is_deleted UInt8
) ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY key;
SYSTEM STOP MERGES t_replacing_is_deleted_04906;

-- One part per INSERT and no OPTIMIZE: the merge must run for the results below to deduplicate,
-- so an arm that stopped reading `ver`/`is_deleted` would return the pre-merge rows.
INSERT INTO t_replacing_is_deleted_04906 VALUES (1, 'test1', 1, 0);
INSERT INTO t_replacing_is_deleted_04906 VALUES (1, 'test2', 2, 0);
INSERT INTO t_replacing_is_deleted_04906 VALUES (2, 'test3', 1, 0);
INSERT INTO t_replacing_is_deleted_04906 VALUES (2, 'test4', 2, 1);
INSERT INTO t_replacing_is_deleted_04906 VALUES (3, 'test5', 1, 1);

SELECT '--- fixture is multi-part';
SELECT count() > 1 AS multi_part FROM system.parts
WHERE database = currentDatabase() AND table = 't_replacing_is_deleted_04906' AND active;

SELECT '--- replacing with is_deleted';
SELECT key, someCol FROM t_replacing_is_deleted_04906 FINAL PREWHERE (ver > 0) OR (database != '') ORDER BY key
SETTINGS query_plan_remove_unused_columns = 1;
SELECT key, someCol FROM t_replacing_is_deleted_04906 FINAL PREWHERE (ver > 0) OR (database != '') ORDER BY key
SETTINGS query_plan_remove_unused_columns = 0;

SELECT '--- through merge()';
SELECT key, someCol FROM merge(currentDatabase(), '^t_replacing_is_deleted_04906$') FINAL PREWHERE (ver > 0) OR (database != '') ORDER BY key
SETTINGS query_plan_remove_unused_columns = 1;

SELECT '--- prewhere applied after final';
SELECT key, someCol FROM t_replacing_is_deleted_04906 FINAL PREWHERE (ver > 0) OR (database != '') ORDER BY key
SETTINGS apply_prewhere_after_final = 1, query_plan_remove_unused_columns = 1;

-- The rows above come from an always-true predicate, so they alone would not notice the setting
-- being ignored. `Deferred prewhere filter column` is printed from `deferred_prewhere_info`, so it
-- is absent unless the filter really was deferred; the second arm is the control showing this
-- oracle can be false. A `FilterTransform` probe over EXPLAIN PIPELINE is not specific to it:
-- `readNonIntersectingFinalWithEngineFilter` adds an `is_deleted` filter whenever a range skips
-- the merge, independently of this setting.
SELECT count() AS deferred_prewhere
FROM (
    EXPLAIN PLAN actions = 1
    SELECT key, someCol FROM t_replacing_is_deleted_04906 FINAL PREWHERE (ver > 0) OR (database != '')
    SETTINGS explain_query_plan_default = 'legacy', apply_prewhere_after_final = 1,
             query_plan_remove_unused_columns = 1
)
WHERE explain LIKE '%Deferred prewhere filter column%';
SELECT count() AS deferred_prewhere
FROM (
    EXPLAIN PLAN actions = 1
    SELECT key, someCol FROM t_replacing_is_deleted_04906 FINAL PREWHERE (ver > 0) OR (database != '')
    SETTINGS explain_query_plan_default = 'legacy', apply_prewhere_after_final = 0,
             query_plan_remove_unused_columns = 1
)
WHERE explain LIKE '%Deferred prewhere filter column%';

-- A selective predicate over the version column: applied after the merge it sees only the winning
-- row per key, applied before it also admits the losing row. The rows differ between the two arms,
-- so the deferral has to happen in the pipeline and not merely be recorded in the plan. The fixture
-- has no is-deleted column, so no engine filter is added on top of the deferred one.
DROP TABLE IF EXISTS t_replacing_deferred_04906;
CREATE TABLE t_replacing_deferred_04906
(
    key Int64,
    someCol String,
    ver UInt64
) ENGINE = ReplacingMergeTree(ver) ORDER BY key;
SYSTEM STOP MERGES t_replacing_deferred_04906;

INSERT INTO t_replacing_deferred_04906 VALUES (1, 'test1', 1);
INSERT INTO t_replacing_deferred_04906 VALUES (1, 'test2', 2);
INSERT INTO t_replacing_deferred_04906 VALUES (2, 'test3', 1);

SELECT '--- fixture is multi-part';
SELECT count() > 1 AS multi_part FROM system.parts
WHERE database = currentDatabase() AND table = 't_replacing_deferred_04906' AND active;

SELECT '--- deferred prewhere selects after the merge';
SELECT key, someCol FROM t_replacing_deferred_04906 FINAL PREWHERE ver = 1 ORDER BY key
SETTINGS apply_prewhere_after_final = 1, query_plan_remove_unused_columns = 1;
SELECT key, someCol FROM t_replacing_deferred_04906 FINAL PREWHERE ver = 1 ORDER BY key
SETTINGS apply_prewhere_after_final = 0, query_plan_remove_unused_columns = 1;

DROP TABLE t_replacing_deferred_04906;

-- Lazy materialization rebuilds the step header once more, so it is pinned rather than left to the
-- 5% of runs that randomize it off. The assertion below shows the lazy step is present.
SELECT '--- lazy materialization';
SELECT key, someCol FROM t_replacing_is_deleted_04906 FINAL PREWHERE (ver > 0) OR (database != '') ORDER BY key LIMIT 1
SETTINGS query_plan_remove_unused_columns = 1, query_plan_optimize_lazy_materialization = 1;
SELECT count() > 0 AS lazy_read_step
FROM (
    EXPLAIN PLAN header = 1
    SELECT key, someCol FROM t_replacing_is_deleted_04906 FINAL PREWHERE (ver > 0) OR (database != '') ORDER BY key LIMIT 1
    SETTINGS explain_query_plan_default = 'legacy', query_plan_remove_unused_columns = 1,
             query_plan_optimize_lazy_materialization = 1
)
WHERE explain LIKE '%LazilyReadFromMergeTree%';
SELECT count() > 0 AS lazy_read_step
FROM (
    EXPLAIN PLAN header = 1
    SELECT key, someCol FROM t_replacing_is_deleted_04906 FINAL PREWHERE (ver > 0) OR (database != '') ORDER BY key LIMIT 1
    SETTINGS explain_query_plan_default = 'legacy', query_plan_remove_unused_columns = 1,
             query_plan_optimize_lazy_materialization = 0
)
WHERE explain LIKE '%LazilyReadFromMergeTree%';

-- Two output columns carrying one name, and one column selected twice: both must keep exactly
-- two columns.
SELECT '--- duplicate output names';
SELECT key, someCol AS key FROM t_replacing_is_deleted_04906 FINAL PREWHERE (ver > 0) OR (database != '') ORDER BY 1, 2
SETTINGS query_plan_remove_unused_columns = 1;
SELECT key, key FROM t_replacing_is_deleted_04906 FINAL PREWHERE (ver > 0) OR (database != '') ORDER BY 1
SETTINGS query_plan_remove_unused_columns = 1;

DROP TABLE t_replacing_is_deleted_04906;

-- The filter reaches PREWHERE through the optimizer rather than the query text. The merging column
-- must be in the sorting key: `MergeTreeWhereOptimizer::cannotBeMoved` refuses to move a non
-- sorting-key table column under FINAL.
DROP TABLE IF EXISTS t_optmoved_04906;
CREATE TABLE t_optmoved_04906
(
    key Int64,
    someCol String,
    ver UInt64,
    is_deleted UInt8
) ENGINE = ReplacingMergeTree(ver, is_deleted) ORDER BY (key, is_deleted);
SYSTEM STOP MERGES t_optmoved_04906;

INSERT INTO t_optmoved_04906 VALUES (1, 'test1', 1, 0);
INSERT INTO t_optmoved_04906 VALUES (1, 'test2', 2, 0);
INSERT INTO t_optmoved_04906 VALUES (2, 'test3', 1, 0);

SELECT '--- fixture is multi-part';
SELECT count() > 1 AS multi_part FROM system.parts
WHERE database = currentDatabase() AND table = 't_optmoved_04906' AND active;

SELECT '--- filter moved to prewhere by the optimizer';
SELECT key, someCol FROM t_optmoved_04906 FINAL WHERE is_deleted = 0 ORDER BY key
SETTINGS query_plan_remove_unused_columns = 1, optimize_move_to_prewhere = 1,
         optimize_move_to_prewhere_if_final = 1, query_plan_optimize_prewhere = 1;
SELECT key, someCol FROM t_optmoved_04906 FINAL WHERE is_deleted = 0 ORDER BY key
SETTINGS query_plan_remove_unused_columns = 0, optimize_move_to_prewhere = 1,
         optimize_move_to_prewhere_if_final = 1, query_plan_optimize_prewhere = 1;

-- The move only happens with `optimize_move_to_prewhere_if_final`, whose default is false, so the
-- second arm is the control showing this oracle can be false.
SELECT count() > 0 AS filter_moved_to_prewhere
FROM (
    EXPLAIN PLAN actions = 1
    SELECT key, someCol FROM t_optmoved_04906 FINAL WHERE is_deleted = 0
    SETTINGS explain_query_plan_default = 'legacy', query_plan_remove_unused_columns = 1,
             optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1,
             query_plan_optimize_prewhere = 1
)
WHERE explain LIKE '%Prewhere filter%';
SELECT count() > 0 AS filter_moved_to_prewhere
FROM (
    EXPLAIN PLAN actions = 1
    SELECT key, someCol FROM t_optmoved_04906 FINAL WHERE is_deleted = 0
    SETTINGS explain_query_plan_default = 'legacy', query_plan_remove_unused_columns = 1,
             optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 0,
             query_plan_optimize_prewhere = 1
)
WHERE explain LIKE '%Prewhere filter%';

DROP TABLE t_optmoved_04906;

DROP TABLE IF EXISTS t_replacing_04906;
CREATE TABLE t_replacing_04906
(
    key Int64,
    someCol String,
    ver UInt64
) ENGINE = ReplacingMergeTree(ver) ORDER BY key;
SYSTEM STOP MERGES t_replacing_04906;

INSERT INTO t_replacing_04906 VALUES (1, 'test1', 1);
INSERT INTO t_replacing_04906 VALUES (1, 'test2', 2);
INSERT INTO t_replacing_04906 VALUES (2, 'test3', 1);

SELECT '--- fixture is multi-part';
SELECT count() > 1 AS multi_part FROM system.parts
WHERE database = currentDatabase() AND table = 't_replacing_04906' AND active;

SELECT '--- replacing without is_deleted';
SELECT key, someCol FROM t_replacing_04906 FINAL PREWHERE (ver > 0) OR (database != '') ORDER BY key
SETTINGS query_plan_remove_unused_columns = 1;
SELECT key, someCol FROM t_replacing_04906 FINAL PREWHERE (ver > 0) OR (database != '') ORDER BY key
SETTINGS query_plan_remove_unused_columns = 0;

-- Pruning must stay live on the shape above: with `ver` no longer read out of the step, the
-- PREWHERE expression folds to a constant and no `ver` input is left in the step's actions. The
-- second arm is the control showing the oracle can be false.
SELECT '--- pruning is still applied';
SELECT count() = 0 AS prewhere_input_pruned
FROM (
    EXPLAIN PLAN actions = 1
    SELECT key, someCol FROM t_replacing_04906 FINAL PREWHERE (ver > 0) OR (database != '')
    SETTINGS explain_query_plan_default = 'legacy', query_plan_remove_unused_columns = 1
)
WHERE explain LIKE '%INPUT%ver%';
SELECT count() = 0 AS prewhere_input_pruned
FROM (
    EXPLAIN PLAN actions = 1
    SELECT key, someCol FROM t_replacing_04906 FINAL PREWHERE (ver > 0) OR (database != '')
    SETTINGS explain_query_plan_default = 'legacy', query_plan_remove_unused_columns = 0
)
WHERE explain LIKE '%INPUT%ver%';

-- A merging column that IS in the output header must still be read, and `someCol` -- consumed by
-- PREWHERE alone -- must still be pruned out of the step header.
SELECT '--- merging column in the output header';
SELECT key, ver FROM t_replacing_04906 FINAL PREWHERE someCol != '' ORDER BY key
SETTINGS query_plan_remove_unused_columns = 1;
SELECT count() = 0 AS somecol_pruned_from_read
FROM (
    EXPLAIN PLAN header = 1
    SELECT key, ver FROM t_replacing_04906 FINAL PREWHERE someCol != ''
    SETTINGS explain_query_plan_default = 'legacy', query_plan_remove_unused_columns = 1
)
WHERE explain LIKE '%someCol%';

-- Plain FINAL with the sorting key and the version unselected: the merge still gets its columns
-- from the pipeline, so the result deduplicates and the output stays one column wide.
SELECT '--- final without prewhere, merging columns unselected';
SELECT someCol FROM t_replacing_04906 FINAL ORDER BY someCol
SETTINGS query_plan_remove_unused_columns = 1;

DROP TABLE t_replacing_04906;

DROP TABLE IF EXISTS t_collapsing_04906;
CREATE TABLE t_collapsing_04906
(
    key Int64,
    someCol String,
    sign Int8
) ENGINE = CollapsingMergeTree(sign) ORDER BY key;
SYSTEM STOP MERGES t_collapsing_04906;

INSERT INTO t_collapsing_04906 VALUES (1, 'test1', 1);
INSERT INTO t_collapsing_04906 VALUES (1, 'test1', -1);
INSERT INTO t_collapsing_04906 VALUES (2, 'test2', 1);
INSERT INTO t_collapsing_04906 VALUES (3, 'test3', 1);

SELECT '--- fixture is multi-part';
SELECT count() > 1 AS multi_part FROM system.parts
WHERE database = currentDatabase() AND table = 't_collapsing_04906' AND active;

SELECT '--- collapsing';
SELECT key, someCol FROM t_collapsing_04906 FINAL PREWHERE (sign = 1) OR (database != '') ORDER BY key
SETTINGS query_plan_remove_unused_columns = 1;
SELECT key, someCol FROM t_collapsing_04906 FINAL PREWHERE (sign = 1) OR (database != '') ORDER BY key
SETTINGS query_plan_remove_unused_columns = 0;

DROP TABLE t_collapsing_04906;

DROP TABLE IF EXISTS t_versioned_collapsing_04906;
CREATE TABLE t_versioned_collapsing_04906
(
    key Int64,
    someCol String,
    sign Int8,
    ver UInt64
) ENGINE = VersionedCollapsingMergeTree(sign, ver) ORDER BY key;
SYSTEM STOP MERGES t_versioned_collapsing_04906;

INSERT INTO t_versioned_collapsing_04906 VALUES (1, 'test1', 1, 1);
INSERT INTO t_versioned_collapsing_04906 VALUES (1, 'test1', -1, 1);
INSERT INTO t_versioned_collapsing_04906 VALUES (2, 'test2', 1, 2);
INSERT INTO t_versioned_collapsing_04906 VALUES (3, 'test3', 1, 1);

SELECT '--- fixture is multi-part';
SELECT count() > 1 AS multi_part FROM system.parts
WHERE database = currentDatabase() AND table = 't_versioned_collapsing_04906' AND active;

SELECT '--- versioned collapsing';
SELECT key, someCol FROM t_versioned_collapsing_04906 FINAL PREWHERE (sign = 1) OR (database != '') ORDER BY key
SETTINGS query_plan_remove_unused_columns = 1;
SELECT key, someCol FROM t_versioned_collapsing_04906 FINAL PREWHERE (sign = 1) OR (database != '') ORDER BY key
SETTINGS query_plan_remove_unused_columns = 0;

DROP TABLE t_versioned_collapsing_04906;
