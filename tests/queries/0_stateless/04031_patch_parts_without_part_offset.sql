-- Regression test for "Min/max part offset must be set in RangeReader for reading patch parts"
-- The bug occurs when a merged data part has BOTH PatchMode::Merge and PatchMode::Join patches.
-- We use apply_patches_on_merge = 0 so OPTIMIZE merges data parts but keeps patches intact.
-- Pre-merge patches become PatchMode::Join, post-merge patches are PatchMode::Merge.
--
-- Note: query_plan_optimize_lazy_materialization is disabled because lazy materializing
-- has a separate bug with patch parts (empty read_sample_block passed to readPatches).

SET enable_lightweight_update = 1;
SET mutations_sync = 2;
SET query_plan_optimize_lazy_materialization = 0;

DROP TABLE IF EXISTS t_lwu_merge_join_patch;

CREATE TABLE t_lwu_merge_join_patch(a UInt64, b UInt64 DEFAULT 0, c Array(String) DEFAULT [])
ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

-- Step 1: Create multiple data parts
INSERT INTO t_lwu_merge_join_patch (a) SELECT number FROM numbers(10000);
INSERT INTO t_lwu_merge_join_patch (a) SELECT number + 10000 FROM numbers(10000);
INSERT INTO t_lwu_merge_join_patch (a) SELECT number + 20000 FROM numbers(10000);

-- Step 2: UPDATE before merge → creates patch parts for individual data parts
UPDATE t_lwu_merge_join_patch SET b = 1 WHERE a % 2 = 0;

-- Step 3: Merge data parts only (apply_patches_on_merge = 0 → patches survive)
-- Old patches now reference pre-merge parts → PatchMode::Join
OPTIMIZE TABLE t_lwu_merge_join_patch FINAL;

-- Step 4: Another UPDATE after merge → PatchMode::Merge patch for the merged part
UPDATE t_lwu_merge_join_patch SET b = 2, c = ['a', 'b', 'c'] WHERE a % 3 = 0;

-- Step 5: This SELECT triggers the bug when both Merge and Join patches exist.
-- Without the fix, fails with "Min/max part offset must be set in RangeReader"
SELECT * FROM t_lwu_merge_join_patch ORDER BY a LIMIT 10;

DROP TABLE IF EXISTS t_lwu_merge_join_patch;
