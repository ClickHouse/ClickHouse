DROP TABLE IF EXISTS t_lwu_lock_profile_events SYNC;

CREATE TABLE t_lwu_lock_profile_events (id UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_lwu_lock_profile_events', '1')
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

SET enable_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update_force';

INSERT INTO t_lwu_lock_profile_events SELECT number FROM numbers(100000);
DELETE FROM t_lwu_lock_profile_events WHERE id < 10000;

SELECT count() FROM t_lwu_lock_profile_events;
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['PatchesAcquireLockTries'], ProfileEvents['PatchesAcquireLockMicroseconds'] > 0
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND query LIKE '%DELETE FROM t_lwu_lock_profile_events WHERE id < 10000%';

DROP TABLE t_lwu_lock_profile_events SYNC;
