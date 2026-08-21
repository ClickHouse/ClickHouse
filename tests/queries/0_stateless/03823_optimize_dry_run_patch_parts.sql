SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;

DROP TABLE IF EXISTS t_dry_run_patches SYNC;

CREATE TABLE t_dry_run_patches (id UInt64, c1 UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    apply_patches_on_merge = 1;

SYSTEM STOP MERGES t_dry_run_patches;

INSERT INTO t_dry_run_patches SELECT number, number FROM numbers(10);
INSERT INTO t_dry_run_patches SELECT number + 10, number + 10 FROM numbers(10);

UPDATE t_dry_run_patches SET c1 = c1 + 100 WHERE id % 2 = 0;

SELECT 'data with patches applied';
SELECT * FROM t_dry_run_patches ORDER BY id;

SELECT 'parts before dry run';
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_dry_run_patches' AND active ORDER BY name;

OPTIMIZE TABLE t_dry_run_patches DRY RUN PARTS 'all_1_1_0', 'all_2_2_0';

-- After DRY RUN, parts and data must remain unchanged: no merge committed.
SELECT 'parts after dry run with patches';
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_dry_run_patches' AND active ORDER BY name;

SELECT 'data after dry run';
SELECT * FROM t_dry_run_patches ORDER BY id;

SYSTEM FLUSH LOGS query_log;
SELECT 'profile events patch parts';

SELECT
    query,
    query_kind,
    ProfileEvents['Merge'],
    ProfileEvents['MergedRows'],
    ProfileEvents['MergeSourceParts'],
    ProfileEvents['MergeWrittenRows'],
    ProfileEvents['PatchesAppliedInAllReadTasks']
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE 'OPTIMIZE TABLE t_dry_run_patches DRY RUN PARTS%' AND type = 'QueryFinish'
ORDER BY event_time_microseconds;

DROP TABLE t_dry_run_patches SYNC;
