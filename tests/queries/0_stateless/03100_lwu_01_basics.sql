-- Tags: no-parallel-replicas, no-replicated-database
-- no-parallel-replicas: profile events may differ with parallel replicas.
-- no-replicated-database: fails due to additional shard.

SET insert_keeper_fault_injection_probability = 0.0;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_shared SYNC;

CREATE TABLE t_shared (id UInt64, c1 UInt64, c2 Int16)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_shared/', '1')
ORDER BY id
SETTINGS
    enable_block_number_column = true,
    enable_block_offset_column = true;

INSERT INTO t_shared SELECT number, number, number FROM numbers(20);
INSERT INTO t_shared SELECT number, number, number FROM numbers(100, 10);

SET apply_patch_parts = 1;
SET max_threads = 1;

UPDATE t_shared SET c2 = c1 * c1 WHERE id % 2 = 0;

SELECT * FROM t_shared ORDER BY id;
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_shared' ORDER BY name;

DETACH TABLE t_shared;
ATTACH TABLE t_shared;

SELECT * FROM t_shared ORDER BY id;
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_shared' ORDER BY name;

ALTER TABLE t_shared APPLY PATCHES SETTINGS mutations_sync = 2;

SELECT * FROM t_shared ORDER BY id;
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 't_shared' ORDER BY name;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['ReadTasksWithAppliedPatches']
FROM system.query_log
WHERE current_database = currentDatabase() AND query = 'SELECT * FROM t_shared ORDER BY id;' AND type = 'QueryFinish'
ORDER BY event_time_microseconds;

DROP TABLE t_shared SYNC;
