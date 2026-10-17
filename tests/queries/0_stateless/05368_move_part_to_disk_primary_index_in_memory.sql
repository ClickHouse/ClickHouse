-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: the storage policy `local_remote` needs S3
-- Tag no-replicated-database: `ALTER TABLE ... MOVE PARTITION ... TO VOLUME` is not replicated

-- Without `use_primary_key_cache`, a part keeps its primary index in memory. With
-- `transfer_caches_on_disk_move` and `primary_key_lazy_load`, a part moved to another disk takes
-- over the index of the part it replaces, of the projection parts too, instead of loading it again
-- from the new disk on the first read.

SET enable_parallel_replicas = 0;
SET alter_move_to_space_execute_async = 0;

DROP TABLE IF EXISTS t_enabled;
DROP TABLE IF EXISTS t_disabled;
DROP TABLE IF EXISTS t_rmt;

CREATE TABLE t_enabled (key UInt64, value UInt64, PROJECTION p_value (SELECT key, value ORDER BY value))
ENGINE = MergeTree ORDER BY key
SETTINGS storage_policy = 'local_remote', index_granularity = 100, min_bytes_for_wide_part = 0,
    use_primary_key_cache = 0, primary_key_lazy_load = 1, transfer_caches_on_disk_move = 1;

CREATE TABLE t_disabled (key UInt64, value UInt64, PROJECTION p_value (SELECT key, value ORDER BY value))
ENGINE = MergeTree ORDER BY key
SETTINGS storage_policy = 'local_remote', index_granularity = 100, min_bytes_for_wide_part = 0,
    use_primary_key_cache = 0, primary_key_lazy_load = 1, transfer_caches_on_disk_move = 0;

CREATE TABLE t_rmt (key UInt64, value UInt64, PROJECTION p_value (SELECT key, value ORDER BY value))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rmt', 'r1') ORDER BY key
SETTINGS storage_policy = 'local_remote', index_granularity = 100, min_bytes_for_wide_part = 0,
    use_primary_key_cache = 0, primary_key_lazy_load = 1, transfer_caches_on_disk_move = 1;

INSERT INTO t_enabled SELECT number, number * 7 % 10000 FROM numbers(10000);
INSERT INTO t_disabled SELECT number, number * 7 % 10000 FROM numbers(10000);
INSERT INTO t_rmt SELECT number, number * 7 % 10000 FROM numbers(10000);

-- A new part keeps the primary index it was written with.
SELECT table, disk_name, primary_key_bytes_in_memory > 0 FROM system.parts
WHERE database = currentDatabase() AND active ORDER BY table;
SELECT table, name, primary_key_bytes_in_memory > 0 FROM system.projection_parts
WHERE database = currentDatabase() AND active ORDER BY table;

ALTER TABLE t_enabled MOVE PARTITION tuple() TO VOLUME 'remote';
ALTER TABLE t_disabled MOVE PARTITION tuple() TO VOLUME 'remote';
ALTER TABLE t_rmt MOVE PARTITION tuple() TO VOLUME 'remote';

-- The moved parts have it as well, unless `transfer_caches_on_disk_move` is disabled: the index is
-- loaded lazily, so the copy made by the move has none of its own.
SELECT table, disk_name, primary_key_bytes_in_memory > 0 FROM system.parts
WHERE database = currentDatabase() AND active ORDER BY table;
SELECT table, name, primary_key_bytes_in_memory > 0 FROM system.projection_parts
WHERE database = currentDatabase() AND active ORDER BY table;

SELECT sum(value) FROM t_enabled WHERE key < 5000 SETTINGS log_comment = '05368:enabled';
SELECT sum(value) FROM t_disabled WHERE key < 5000 SETTINGS log_comment = '05368:disabled';
SELECT sum(value) FROM t_rmt WHERE key < 5000 SETTINGS log_comment = '05368:rmt';
SELECT sum(key) FROM t_enabled WHERE value < 5000
SETTINGS log_comment = '05368:enabled_projection', optimize_use_projections = 1, force_optimize_projection = 1;

-- And back to the local disk.
ALTER TABLE t_enabled MOVE PARTITION tuple() TO VOLUME 'local';

SELECT table, disk_name, primary_key_bytes_in_memory > 0 FROM system.parts
WHERE database = currentDatabase() AND active AND table = 't_enabled';

SELECT sum(value) FROM t_enabled WHERE key < 5000 SETTINGS log_comment = '05368:enabled_back_to_local';

SYSTEM FLUSH LOGS query_log;

-- Only the read of `t_disabled` loads a primary index.
SELECT replaceOne(log_comment, '05368:', '') AS read, ProfileEvents['LoadedPrimaryIndexFiles'] > 0 AS loaded_primary_index
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment LIKE '05368:%'
ORDER BY read;

DROP TABLE t_enabled;
DROP TABLE t_disabled;
DROP TABLE t_rmt;
