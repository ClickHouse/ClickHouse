-- Tags: no-parallel-replicas, no-replicated-database, long
-- no-parallel-replicas: profile events may differ with parallel replicas.
-- no-replicated-database: fails due to additional shard.

SET insert_keeper_fault_injection_probability = 0.0;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_shared SYNC;

CREATE TABLE t_shared (id UInt64, c1 UInt64)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_shared/', '1')
ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    apply_patches_on_merge = 0;

INSERT INTO t_shared VALUES (1, 2) (3, 4);

UPDATE t_shared SET c1 = 100 WHERE id = 1;

SELECT name, rows from system.parts WHERE database = currentDatabase() AND table = 't_shared' AND active ORDER BY name;

SELECT * FROM t_shared ORDER BY id;

INSERT INTO t_shared VALUES (5, 6);

UPDATE t_shared SET c1 = 200 WHERE id = 5;

SELECT name, rows from system.parts WHERE database = currentDatabase() AND table = 't_shared' AND active ORDER BY name;

SELECT * FROM t_shared ORDER BY id;

OPTIMIZE TABLE t_shared PARTITION ID 'all' FINAL;

SELECT name, rows from system.parts WHERE database = currentDatabase() AND table = 't_shared' AND active ORDER BY name;

SELECT * FROM t_shared ORDER BY id;

DROP TABLE t_shared SYNC;

SYSTEM FLUSH LOGS query_log;

SELECT mapSort(mapFilter((k, v) -> k IN ('ReadTasksWithAppliedPatches', 'PatchesAppliedInAllReadTasks', 'PatchesMergeAppliedInAllReadTasks', 'PatchesJoinAppliedInAllReadTasks'), ProfileEvents))
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE '%SELECT * FROM t_shared ORDER BY id%' AND type = 'QueryFinish'
ORDER BY event_time_microseconds;
