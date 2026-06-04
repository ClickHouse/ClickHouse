-- Tags: no-random-settings, no-random-merge-tree-settings, no-shared-merge-tree

SET allow_experimental_lightweight_update = 1;

-- With apply_patches_on_merge (default on), a merge attaches pending patch parts and folds their
-- version into the result name (e.g. all_1_2_1 -> all_1_2_1_4). The manual selector predicts the
-- no-patch name, so the produced part would never match what was scheduled. Scheduling is rejected.
DROP TABLE IF EXISTS t_manual_patch;

CREATE TABLE t_manual_patch (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS merge_selector_algorithm = 'Manual', enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_manual_patch VALUES (1, 1);
INSERT INTO t_manual_patch VALUES (2, 2);
INSERT INTO t_manual_patch VALUES (3, 3);

UPDATE t_manual_patch SET v = 99 WHERE id = 1;

SYSTEM SCHEDULE MERGE t_manual_patch PARTS 'all_1_1_0', 'all_2_2_0'; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_manual_patch;

-- With apply_patches_on_merge off the merge does not attach patches (they keep applying on the fly
-- until materialized later), so the result name is deterministic and scheduling works as usual.
DROP TABLE IF EXISTS t_manual_patch_off;

CREATE TABLE t_manual_patch_off (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS merge_selector_algorithm = 'Manual', enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

INSERT INTO t_manual_patch_off VALUES (1, 1);
INSERT INTO t_manual_patch_off VALUES (2, 2);
INSERT INTO t_manual_patch_off VALUES (3, 3);

UPDATE t_manual_patch_off SET v = 99 WHERE id = 1;

SYSTEM SCHEDULE MERGE t_manual_patch_off PARTS 'all_1_1_0', 'all_2_2_0';
SYSTEM SYNC MERGES t_manual_patch_off;

SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_manual_patch_off' AND active AND partition_id = 'all' ORDER BY name;

DROP TABLE t_manual_patch_off;
