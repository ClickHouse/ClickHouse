-- Tags: no-replicated-database
-- no-replicated-database: read_rows in query_log differs because of replicated database.

DROP TABLE IF EXISTS t_lwd_index SYNC;

CREATE TABLE t_lwd_index (id UInt64)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwd_index/', '1')
ORDER BY id
SETTINGS index_granularity = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwd_index SELECT * FROM numbers(1000);

SET enable_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update_force';

DELETE FROM t_lwd_index WHERE id = 200;
DELETE FROM t_lwd_index WHERE id IN (100, 110, 120, 130);

SYSTEM FLUSH LOGS query_log;

SELECT read_rows FROM system.query_log
WHERE type = 'QueryFinish' AND query like 'DELETE FROM t_lwd_index%' AND current_database = currentDatabase()
ORDER BY event_time_microseconds;

DROP TABLE t_lwd_index;
