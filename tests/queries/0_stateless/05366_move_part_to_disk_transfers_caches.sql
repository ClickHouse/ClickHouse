-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: the storage policy `local_remote` needs S3
-- Tag no-replicated-database: `ALTER TABLE ... MOVE PARTITION ... TO VOLUME` is not replicated

-- With `transfer_caches_on_disk_move`, a part moved to another disk keeps its entries in the mark
-- cache, the index mark cache and the primary index cache: the first read after the move finds
-- them under the keys of the moved part. Without it, that read loads them again.

SET load_marks_asynchronously = 0;
SET allow_prefetched_read_pool_for_remote_filesystem = 0;
SET allow_prefetched_read_pool_for_local_filesystem = 0;
SET enable_parallel_replicas = 0;
SET use_query_condition_cache = 0;
SET use_skip_indexes = 1;
SET alter_move_to_space_execute_async = 0;

DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_rmt;
DROP TABLE IF EXISTS t_projection;
DROP TABLE IF EXISTS t_disabled;

-- `prewarm_primary_key_cache` puts the primary index of a new part into the cache: otherwise the
-- part keeps the index it was written with in memory and does not use the cache.

CREATE TABLE t_wide (key UInt64, value UInt64, INDEX idx_value value TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY key
SETTINGS storage_policy = 'local_remote', index_granularity = 100, min_bytes_for_wide_part = 0,
    use_primary_key_cache = 1, primary_key_lazy_load = 1, prewarm_primary_key_cache = 1, transfer_caches_on_disk_move = 1;

CREATE TABLE t_compact (key UInt64, value UInt64, INDEX idx_value value TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY key
SETTINGS storage_policy = 'local_remote', index_granularity = 100, min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000,
    use_primary_key_cache = 1, primary_key_lazy_load = 1, prewarm_primary_key_cache = 1, transfer_caches_on_disk_move = 1;

CREATE TABLE t_rmt (key UInt64, value UInt64, INDEX idx_value value TYPE minmax GRANULARITY 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rmt', 'r1') ORDER BY key
SETTINGS storage_policy = 'local_remote', index_granularity = 100, min_bytes_for_wide_part = 0,
    use_primary_key_cache = 1, primary_key_lazy_load = 1, prewarm_primary_key_cache = 1, transfer_caches_on_disk_move = 1;

CREATE TABLE t_projection (key UInt64, value UInt64, PROJECTION p_value (SELECT key, value ORDER BY value))
ENGINE = MergeTree ORDER BY key
SETTINGS storage_policy = 'local_remote', index_granularity = 100, min_bytes_for_wide_part = 0,
    use_primary_key_cache = 1, primary_key_lazy_load = 1, prewarm_primary_key_cache = 1, transfer_caches_on_disk_move = 1;

CREATE TABLE t_disabled (key UInt64, value UInt64, INDEX idx_value value TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY key
SETTINGS storage_policy = 'local_remote', index_granularity = 100, min_bytes_for_wide_part = 0,
    use_primary_key_cache = 1, primary_key_lazy_load = 1, prewarm_primary_key_cache = 1, transfer_caches_on_disk_move = 0;

INSERT INTO t_wide SELECT number, number * 7 % 10000 FROM numbers(10000);
INSERT INTO t_compact SELECT number, number * 7 % 10000 FROM numbers(10000);
INSERT INTO t_rmt SELECT number, number * 7 % 10000 FROM numbers(10000);
INSERT INTO t_projection SELECT number, number * 7 % 10000 FROM numbers(10000);
INSERT INTO t_disabled SELECT number, number * 7 % 10000 FROM numbers(10000);

-- The reads use the primary index, the marks of the skip index and the marks of the columns.
-- The read of `t_projection` uses the primary index and the marks of the projection part.

SELECT sum(value) FROM t_wide WHERE key < 5000 AND value < 5000 SETTINGS log_comment = '05366:wide_warm';
SELECT sum(value) FROM t_compact WHERE key < 5000 AND value < 5000 SETTINGS log_comment = '05366:compact_warm';
SELECT sum(value) FROM t_rmt WHERE key < 5000 AND value < 5000 SETTINGS log_comment = '05366:rmt_warm';
SELECT sum(key) FROM t_projection WHERE value < 5000 SETTINGS log_comment = '05366:projection_warm', optimize_use_projections = 1, force_optimize_projection = 1;
SELECT sum(value) FROM t_disabled WHERE key < 5000 AND value < 5000 SETTINGS log_comment = '05366:disabled_warm';

ALTER TABLE t_wide MOVE PARTITION tuple() TO VOLUME 'remote';
ALTER TABLE t_compact MOVE PARTITION tuple() TO VOLUME 'remote';
ALTER TABLE t_rmt MOVE PARTITION tuple() TO VOLUME 'remote';
ALTER TABLE t_projection MOVE PARTITION tuple() TO VOLUME 'remote';
ALTER TABLE t_disabled MOVE PARTITION tuple() TO VOLUME 'remote';

SELECT table, part_type, disk_name FROM system.parts
WHERE database = currentDatabase() AND active ORDER BY table;

SELECT sum(value) FROM t_wide WHERE key < 5000 AND value < 5000 SETTINGS log_comment = '05366:wide_to_remote';
SELECT sum(value) FROM t_compact WHERE key < 5000 AND value < 5000 SETTINGS log_comment = '05366:compact_to_remote';
SELECT sum(value) FROM t_rmt WHERE key < 5000 AND value < 5000 SETTINGS log_comment = '05366:rmt_to_remote';
SELECT sum(key) FROM t_projection WHERE value < 5000 SETTINGS log_comment = '05366:projection_to_remote', optimize_use_projections = 1, force_optimize_projection = 1;
SELECT sum(value) FROM t_disabled WHERE key < 5000 AND value < 5000 SETTINGS log_comment = '05366:disabled_to_remote';

-- And back to the local disk.

ALTER TABLE t_wide MOVE PARTITION tuple() TO VOLUME 'local';
ALTER TABLE t_rmt MOVE PARTITION tuple() TO VOLUME 'local';
ALTER TABLE t_projection MOVE PARTITION tuple() TO VOLUME 'local';

SELECT table, part_type, disk_name FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('t_wide', 't_rmt', 't_projection') ORDER BY table;

SELECT sum(value) FROM t_wide WHERE key < 5000 AND value < 5000 SETTINGS log_comment = '05366:wide_to_local';
SELECT sum(value) FROM t_rmt WHERE key < 5000 AND value < 5000 SETTINGS log_comment = '05366:rmt_to_local';
SELECT sum(key) FROM t_projection WHERE value < 5000 SETTINGS log_comment = '05366:projection_to_local', optimize_use_projections = 1, force_optimize_projection = 1;

SYSTEM FLUSH LOGS query_log;

-- The reads after the moves: only those of `t_disabled` have to load anything again.
SELECT
    replaceOne(log_comment, '05366:', '') AS read,
    ProfileEvents['MarkCacheMisses'] > 0 AS mark_cache_misses,
    ProfileEvents['PrimaryIndexCacheMisses'] > 0 AS primary_index_cache_misses
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment LIKE '05366:%' AND log_comment NOT LIKE '%\\_warm'
ORDER BY read;

-- And the others found the entries.
SELECT
    replaceOne(log_comment, '05366:', '') AS read,
    ProfileEvents['MarkCacheHits'] > 0 AS mark_cache_hits,
    ProfileEvents['PrimaryIndexCacheHits'] > 0 AS primary_index_cache_hits
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment LIKE '05366:%' AND log_comment NOT LIKE '%\\_warm' AND log_comment NOT LIKE '%disabled%'
ORDER BY read;

DROP TABLE t_wide;
DROP TABLE t_compact;
DROP TABLE t_rmt;
DROP TABLE t_projection;
DROP TABLE t_disabled;
