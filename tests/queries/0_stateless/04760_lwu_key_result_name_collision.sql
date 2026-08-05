-- Tags: no-parallel-replicas, no-replicated-database
-- no-parallel-replicas: profile events may differ with parallel replicas.
-- no-replicated-database: fails due to additional shard.

-- v2 patch parts store only the input columns of the sorting key expression as row identity.
-- A regular column that shares its name with a result of a key expression (`intHash32(id)`)
-- can be updated and must not be treated as row identity.

SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;

DROP TABLE IF EXISTS t_lwu_key_result_name SYNC;

CREATE TABLE t_lwu_key_result_name (id UInt64, `intHash32(id)` UInt32, v String)
ENGINE = MergeTree
ORDER BY intHash32(id)
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    apply_patches_on_merge = 0,
    patch_parts_version = 'v2';

INSERT INTO t_lwu_key_result_name SELECT number, 0, 'foo' FROM numbers(1000);

-- The patch stores `id` as row identity and `intHash32(id)` as an updated column.
UPDATE t_lwu_key_result_name SET `intHash32(id)` = 1 WHERE id < 100;

-- Reading only the key input column must not apply the patch.
SELECT sum(id) FROM t_lwu_key_result_name;

-- Reading the updated column must apply the patch.
SELECT sum(`intHash32(id)`) FROM t_lwu_key_result_name;

DROP TABLE t_lwu_key_result_name SYNC;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['ReadTasksWithAppliedPatches'] > 0, ProfileEvents['PatchesAppliedInAllReadTasks'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND query LIKE '%SELECT sum(%FROM t_lwu_key_result_name%' AND query NOT LIKE '%query_log%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds;
