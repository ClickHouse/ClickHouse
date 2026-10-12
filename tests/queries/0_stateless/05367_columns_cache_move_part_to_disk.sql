-- Tags: no-parallel, no-random-settings, no-fasttest, no-replicated-database
-- Tag no-parallel, no-random-settings: like the other tests of the columns cache, the ranges it writes depend on other queries and on the settings of the read
-- Tag no-fasttest: the storage policy `local_remote` needs S3
-- Tag no-replicated-database: `ALTER TABLE ... MOVE PARTITION ... TO VOLUME` is not replicated

-- The entries of the columns cache are keyed by the name of the part, not by its disk, so the copy of
-- a part moved to another disk uses the entries of the part it replaces. Destroying the replaced part
-- must not remove them, whether the other caches are transferred (`transfer_caches_on_disk_move`) or not.

SET use_columns_cache = 1;
SET enable_parallel_replicas = 0;
SET max_threads = 1;
SET alter_move_to_space_execute_async = 0;

SYSTEM DROP COLUMNS CACHE;

DROP TABLE IF EXISTS t_columns_cache_mt;
DROP TABLE IF EXISTS t_columns_cache_rmt;

CREATE TABLE t_columns_cache_mt (id UInt64, value String)
ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 'local_remote', min_bytes_for_wide_part = 0, index_granularity = 1000, transfer_caches_on_disk_move = 0;

CREATE TABLE t_columns_cache_rmt (id UInt64, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_columns_cache_rmt', 'r1') ORDER BY id
SETTINGS storage_policy = 'local_remote', min_bytes_for_wide_part = 0, index_granularity = 1000, transfer_caches_on_disk_move = 1;

INSERT INTO t_columns_cache_mt SELECT number, toString(number) FROM numbers(10000);
INSERT INTO t_columns_cache_rmt SELECT number, toString(number) FROM numbers(10000);

SELECT sum(id), sum(length(value)) FROM t_columns_cache_mt;
SELECT sum(id), sum(length(value)) FROM t_columns_cache_rmt;

SELECT table, count() > 0 FROM system.columns_cache
WHERE database = currentDatabase() GROUP BY table ORDER BY table;

ALTER TABLE t_columns_cache_mt MOVE PARTITION tuple() TO VOLUME 'remote';
ALTER TABLE t_columns_cache_rmt MOVE PARTITION tuple() TO VOLUME 'remote';

SELECT table, disk_name FROM system.parts
WHERE database = currentDatabase() AND active ORDER BY table;

-- The replaced parts are destroyed by now, and the entries are still there.
SELECT table, count() > 0 FROM system.columns_cache
WHERE database = currentDatabase() GROUP BY table ORDER BY table;

SELECT sum(id), sum(length(value)) FROM t_columns_cache_mt SETTINGS log_comment = '05367:mt_after_move';
SELECT sum(id), sum(length(value)) FROM t_columns_cache_rmt SETTINGS log_comment = '05367:rmt_after_move';

SYSTEM FLUSH LOGS query_log;

SELECT
    replaceOne(log_comment, '05367:', '') AS read,
    ProfileEvents['ColumnsCacheHits'] > 0 AS hits,
    ProfileEvents['ColumnsCacheMisses'] = 0 AS no_misses
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment LIKE '05367:%'
ORDER BY read;

DROP TABLE t_columns_cache_mt;
DROP TABLE t_columns_cache_rmt;
