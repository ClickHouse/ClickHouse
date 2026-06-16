-- Tags: no-random-merge-tree-settings, no-shared-merge-tree
-- ^ no-shared-merge-tree: MODIFY ENGINE is not supported for Replicated/Shared MergeTree yet.

-- ALTER TABLE ... MODIFY ENGINE changes a MergeTree-family table's engine in place (issue #107551).
-- It rewrites only the merge semantics (MergingParams); ORDER BY / columns are untouched. The change
-- is persisted into the CREATE query and takes effect when the storage is next loaded, so the test
-- DETACHes and ATTACHes the table to observe the new engine's behavior.

SET allow_experimental_alter_modify_engine = 0;

DROP TABLE IF EXISTS t_engine;
CREATE TABLE t_engine (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;

-- Gated behind the experimental setting.
ALTER TABLE t_engine MODIFY ENGINE = ReplacingMergeTree; -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_alter_modify_engine = 1;

-- MergeTree -> ReplacingMergeTree. Duplicate ORDER BY keys survive until FINAL collapses them.
INSERT INTO t_engine VALUES (1, 10);
INSERT INTO t_engine VALUES (1, 20);
INSERT INTO t_engine VALUES (2, 5);
SELECT 'before', count() FROM t_engine;

ALTER TABLE t_engine MODIFY ENGINE = ReplacingMergeTree;
DETACH TABLE t_engine;
ATTACH TABLE t_engine;
SELECT 'engine', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_engine';
SELECT 'replacing final', count() FROM t_engine FINAL;

DROP TABLE t_engine;

-- MergeTree -> SummingMergeTree. FINAL collapses the per-key rows into one summed row. Selecting v
-- directly (not sum(v)) proves the engine changed: plain MergeTree FINAL would keep both rows for k=1.
CREATE TABLE t_sum (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_sum VALUES (1, 10);
INSERT INTO t_sum VALUES (1, 20);
INSERT INTO t_sum VALUES (2, 100);
ALTER TABLE t_sum MODIFY ENGINE = SummingMergeTree;
DETACH TABLE t_sum;
ATTACH TABLE t_sum;
SELECT 'summing', k, v FROM t_sum FINAL ORDER BY k;
DROP TABLE t_sum;

-- Adding the engine-required column in the same statement, before MODIFY ENGINE.
CREATE TABLE t_collapse (a UInt32) ENGINE = MergeTree ORDER BY a;
ALTER TABLE t_collapse ADD COLUMN sign Int8 DEFAULT 1, MODIFY ENGINE = CollapsingMergeTree(sign);
DETACH TABLE t_collapse;
ATTACH TABLE t_collapse;
SELECT 'collapsing', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_collapse';
DROP TABLE t_collapse;

-- Validation: the target must be a MergeTree-family engine and its required columns must exist.
CREATE TABLE t_bad (a UInt32) ENGINE = MergeTree ORDER BY a;
ALTER TABLE t_bad MODIFY ENGINE = Log; -- { serverError UNKNOWN_STORAGE }
ALTER TABLE t_bad MODIFY ENGINE = CollapsingMergeTree(missing); -- { serverError NO_SUCH_COLUMN_IN_TABLE }
DROP TABLE t_bad;

-- Old-syntax tables keep the key/granularity as positional engine arguments. Rewriting the engine
-- clause would drop them and leave an unloadable CREATE query, so MODIFY ENGINE rejects them.
SET allow_deprecated_syntax_for_merge_tree = 1;
CREATE TABLE t_old (d Date, k UInt32, v UInt32) ENGINE = MergeTree(d, k, 8192);
ALTER TABLE t_old MODIFY ENGINE = ReplacingMergeTree(v); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_old;

-- allow_tuple_element_aggregation (Summing/Aggregating/Coalescing only) is derived from the table's
-- final MergeTree settings, not from the engine clause. MODIFY ENGINE must validate the candidate
-- against it, otherwise a table with a Tuple sorting key would pass the ALTER and fail on next ATTACH.

-- (a) setting already on the table: switching to Summing makes the Tuple sorting key illegal.
CREATE TABLE t_tea (k Tuple(UInt32, UInt32), v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS allow_tuple_element_aggregation = 1;
ALTER TABLE t_tea MODIFY ENGINE = SummingMergeTree; -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_tea;

-- (b) setting flipped on in the same statement: validated against the in-flight value.
CREATE TABLE t_tea (k Tuple(UInt32, UInt32), v UInt64) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_tea MODIFY ENGINE = AggregatingMergeTree, MODIFY SETTING allow_tuple_element_aggregation = 1; -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_tea;

-- (c) the flag is ignored for non-aggregating engines, so switching to Replacing stays allowed.
CREATE TABLE t_tea (k Tuple(UInt32, UInt32), v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS allow_tuple_element_aggregation = 1;
ALTER TABLE t_tea MODIFY ENGINE = ReplacingMergeTree(v);
DETACH TABLE t_tea;
ATTACH TABLE t_tea;
SELECT 'tuple-key replacing', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_tea';
DROP TABLE t_tea;

-- A reload-only MODIFY ENGINE leaves the new engine pending on the live metadata while merging_params
-- stays the old mode until reload. A subsequent ALTER with no MODIFY ENGINE of its own must re-validate
-- that pending engine before changing any metadata, otherwise it can persist an unloadable CREATE.

-- (d) a later MODIFY SETTING that invalidates the pending engine is rejected; the table stays loadable.
CREATE TABLE t_pending (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY (k, v)
    SETTINGS allow_summing_columns_in_partition_or_order_key = 1;
ALTER TABLE t_pending MODIFY ENGINE = SummingMergeTree(v);
ALTER TABLE t_pending MODIFY SETTING allow_summing_columns_in_partition_or_order_key = 0; -- { serverError BAD_ARGUMENTS }
DETACH TABLE t_pending;
ATTACH TABLE t_pending;
SELECT 'pending setting guard', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_pending';
DROP TABLE t_pending;

-- (e) dropping the summing column of the pending engine is rejected before the live metadata changes.
CREATE TABLE t_pending (k UInt32, v UInt64, w UInt64) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_pending MODIFY ENGINE = SummingMergeTree(v);
ALTER TABLE t_pending DROP COLUMN v; -- { serverError NO_SUCH_COLUMN_IN_TABLE }
DETACH TABLE t_pending;
ATTACH TABLE t_pending;
SELECT 'pending drop guard', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_pending';
DROP TABLE t_pending;

-- (f) an unrelated later ALTER on a table with a pending engine still works.
CREATE TABLE t_pending (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_pending MODIFY ENGINE = SummingMergeTree(v);
ALTER TABLE t_pending ADD COLUMN z UInt8;
DETACH TABLE t_pending;
ATTACH TABLE t_pending;
SELECT 'pending unrelated alter', engine FROM system.tables WHERE database = currentDatabase() AND name = 't_pending';
DROP TABLE t_pending;
