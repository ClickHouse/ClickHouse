-- Tags: no-fasttest
-- no-fasttest: needs object storage disks (MinIO).

-- `system.remote_data_paths` walks `store` and `data` on every disk it is given and reads the
-- metadata of every file it finds, so a `disk_name` predicate is pushed into the set of disks the
-- source traverses. These are the cases that pruning must get right; that the requested disk is
-- still traversed at all is also covered by `03000_traverse_shadow_system_data_paths`, and that a
-- query without a `disk_name` predicate keeps every disk by `test_disk_over_web_server`.
--
-- Every query below names a disk on purpose: an unfiltered read of this table costs a full
-- traversal of every object storage disk on the server, which is minutes when one of them holds a
-- large table.

DROP TABLE IF EXISTS t_05100;

CREATE TABLE t_05100 (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 's3_no_cache';
INSERT INTO t_05100 SELECT number FROM numbers(10);

-- The table's own files are found through the filtered read.
SELECT 'own disk', count() > 0
FROM system.remote_data_paths
WHERE disk_name = 's3_no_cache'
    AND local_path LIKE concat('%', (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 't_05100'), '%');

-- Pruning is per disk: asking for another disk must not report this table's files.
SELECT 'other disk', count()
FROM system.remote_data_paths
WHERE disk_name = 's3_disk'
    AND local_path LIKE concat('%', (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 't_05100'), '%');

-- A name that no disk has prunes everything.
SELECT 'no such disk', count() FROM system.remote_data_paths WHERE disk_name = 'no_such_disk_05100';

-- `IN` must keep the disk that exists, next to one that does not.
SELECT 'in list', count() > 0
FROM system.remote_data_paths
WHERE disk_name IN ('no_such_disk_05100', 's3_no_cache')
    AND local_path LIKE concat('%', (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 't_05100'), '%');

-- The same read, expressed so that the pruning has to evaluate a function of `disk_name`.
SELECT 'expression', count() > 0
FROM system.remote_data_paths
WHERE startsWith(disk_name, 's3_no_cache')
    AND local_path LIKE concat('%', (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 't_05100'), '%');

DROP TABLE t_05100;
