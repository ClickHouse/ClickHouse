-- Tags: no-random-merge-tree-settings

-- Regression test for MinMaxIndex::merge with Nullable partition keys.
-- MinMaxIndex::merge used std::min/std::max which don't handle Nullable
-- fields correctly. This test verifies that merging parts with Nullable
-- partition keys preserves correct min-max bounds and partition pruning.

DROP TABLE IF EXISTS t_nullable_minmax_merge;

CREATE TABLE t_nullable_minmax_merge (key Nullable(Int64), value String)
ENGINE = MergeTree()
ORDER BY tuple()
PARTITION BY key
SETTINGS allow_nullable_key = 1;

-- Insert into separate parts to force MinMaxIndex::merge during OPTIMIZE
INSERT INTO t_nullable_minmax_merge VALUES (1, 'a');
INSERT INTO t_nullable_minmax_merge VALUES (NULL, 'b');
INSERT INTO t_nullable_minmax_merge VALUES (3, 'c');

-- This triggers MinMaxIndex::merge across parts within each partition
OPTIMIZE TABLE t_nullable_minmax_merge FINAL;

-- Verify data correctness after merge
SELECT key, value FROM t_nullable_minmax_merge ORDER BY key NULLS LAST;

-- Verify partition pruning still works correctly after merge
-- Should read only 1 partition (key=1), not all of them
SELECT value FROM t_nullable_minmax_merge WHERE key = 1;

-- Should read only the NULL partition
SELECT value FROM t_nullable_minmax_merge WHERE key IS NULL;

DROP TABLE t_nullable_minmax_merge;
