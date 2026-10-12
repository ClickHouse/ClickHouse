-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: the storage policy `local_remote` needs S3
-- Tag no-replicated-database: `ALTER TABLE ... MOVE PARTITION ... TO VOLUME` is not replicated

-- A removed part removes the marks of its projection parts from the mark cache. The directory of a
-- projection part is `<part>/<projection>.proj/`, and the key the marks were removed by used to be
-- `<part>/<projection>/`, so they stayed in the cache until evicted.
--
-- To see it, move a part away and back with `transfer_caches_on_disk_move` disabled: the part comes
-- back to its old path on the same disk, so it would find the marks the replaced part left in the
-- cache. As they were removed with the replaced part, the read of the projection loads them again.

SET load_marks_asynchronously = 0;
SET allow_prefetched_read_pool_for_remote_filesystem = 0;
SET allow_prefetched_read_pool_for_local_filesystem = 0;
SET enable_parallel_replicas = 0;
SET use_query_condition_cache = 0;
SET alter_move_to_space_execute_async = 0;

DROP TABLE IF EXISTS t_projection_marks;
DROP TABLE IF EXISTS t_projection_marks_compact;

CREATE TABLE t_projection_marks (key UInt64, value UInt64, PROJECTION p_value (SELECT key, value ORDER BY value))
ENGINE = MergeTree ORDER BY key
SETTINGS storage_policy = 'local_remote', index_granularity = 100, min_bytes_for_wide_part = 0, transfer_caches_on_disk_move = 0;

CREATE TABLE t_projection_marks_compact (key UInt64, value UInt64, PROJECTION p_value (SELECT key, value ORDER BY value))
ENGINE = MergeTree ORDER BY key
SETTINGS storage_policy = 'local_remote', index_granularity = 100, min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000,
    transfer_caches_on_disk_move = 0;

INSERT INTO t_projection_marks SELECT number, number * 7 % 10000 FROM numbers(10000);
INSERT INTO t_projection_marks_compact SELECT number, number * 7 % 10000 FROM numbers(10000);

-- Load the marks of the projection parts into the cache.
SELECT sum(key) FROM t_projection_marks WHERE value < 5000 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT sum(key) FROM t_projection_marks_compact WHERE value < 5000 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

ALTER TABLE t_projection_marks MOVE PARTITION tuple() TO VOLUME 'remote';
ALTER TABLE t_projection_marks MOVE PARTITION tuple() TO VOLUME 'local';
ALTER TABLE t_projection_marks_compact MOVE PARTITION tuple() TO VOLUME 'remote';
ALTER TABLE t_projection_marks_compact MOVE PARTITION tuple() TO VOLUME 'local';

SELECT table, part_type, disk_name FROM system.parts
WHERE database = currentDatabase() AND active ORDER BY table;

SELECT sum(key) FROM t_projection_marks WHERE value < 5000 SETTINGS log_comment = '05369:wide_after', optimize_use_projections = 1, force_optimize_projection = 1;
SELECT sum(key) FROM t_projection_marks_compact WHERE value < 5000 SETTINGS log_comment = '05369:compact_after', optimize_use_projections = 1, force_optimize_projection = 1;

SYSTEM FLUSH LOGS query_log;

SELECT replaceOne(log_comment, '05369:', '') AS read, ProfileEvents['MarkCacheMisses'] > 0 AS mark_cache_misses
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment LIKE '05369:%'
ORDER BY read;

DROP TABLE t_projection_marks;
DROP TABLE t_projection_marks_compact;
