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

-- MergeTree -> SummingMergeTree. FINAL sums the measure columns within each key.
CREATE TABLE t_sum (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_sum VALUES (1, 10);
INSERT INTO t_sum VALUES (1, 20);
INSERT INTO t_sum VALUES (2, 100);
ALTER TABLE t_sum MODIFY ENGINE = SummingMergeTree;
DETACH TABLE t_sum;
ATTACH TABLE t_sum;
SELECT 'summing', k, sum(v) FROM t_sum FINAL GROUP BY k ORDER BY k;
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
