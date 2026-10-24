-- Tags: no-random-settings, no-random-merge-tree-settings, no-shared-merge-tree

SET allow_experimental_lightweight_update = 1;

-- A manual merge does not apply patch parts, so its result name is determined only by the
-- scheduled data parts (no patch-version suffix) and stays predictable for chained merges. The
-- patch keeps being applied on the fly on reads. Here a patch exists before scheduling.
DROP TABLE IF EXISTS t_manual_patch;

CREATE TABLE t_manual_patch (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS merge_selector_algorithm = 'Manual', enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_manual_patch VALUES (1, 1);
INSERT INTO t_manual_patch VALUES (2, 2);
INSERT INTO t_manual_patch VALUES (3, 3);

UPDATE t_manual_patch SET v = 99 WHERE id = 1;

SYSTEM SCHEDULE MERGE t_manual_patch PARTS 'all_1_1_0', 'all_2_2_0';
SYSTEM SCHEDULE MERGE t_manual_patch PARTS 'all_1_2_1', 'all_3_3_0';
SYSTEM SYNC MERGES t_manual_patch;

SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_manual_patch' AND active AND partition_id = 'all' ORDER BY name;
SELECT id, v FROM t_manual_patch ORDER BY id;

DROP TABLE t_manual_patch;

-- Same, but the patch is created AFTER the chained merges are already scheduled (merges stopped so
-- both schedules land first). Because manual merges never attach patches, the produced names still
-- match what was scheduled and SYNC MERGES completes instead of waiting for a patch-suffixed name.
DROP TABLE IF EXISTS t_manual_patch_toctou;

CREATE TABLE t_manual_patch_toctou (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS merge_selector_algorithm = 'Manual', enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_manual_patch_toctou VALUES (1, 1);
INSERT INTO t_manual_patch_toctou VALUES (2, 2);
INSERT INTO t_manual_patch_toctou VALUES (3, 3);

SYSTEM STOP MERGES t_manual_patch_toctou;
SYSTEM SCHEDULE MERGE t_manual_patch_toctou PARTS 'all_1_1_0', 'all_2_2_0';
SYSTEM SCHEDULE MERGE t_manual_patch_toctou PARTS 'all_1_2_1', 'all_3_3_0';
UPDATE t_manual_patch_toctou SET v = 99 WHERE id = 1;
SYSTEM START MERGES t_manual_patch_toctou;
SYSTEM SYNC MERGES t_manual_patch_toctou;

SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_manual_patch_toctou' AND active AND partition_id = 'all' ORDER BY name;
SELECT id, v FROM t_manual_patch_toctou ORDER BY id;

DROP TABLE t_manual_patch_toctou;
