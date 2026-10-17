-- Tags: no-object-storage, no-random-merge-tree-settings
-- no-object-storage: object storage has no fsync, so FileSync stays 0 there.

-- With fsync_after_insert = 1 and fsync_after_insert_each_part = 1 every part has to be durable
-- when it becomes visible, and that includes the projections written together with it on INSERT:
-- their files must be synced as well, otherwise the part can lose its projections on a power loss.

DROP TABLE IF EXISTS t_fsync_no_projection;
DROP TABLE IF EXISTS t_fsync_projection;

CREATE TABLE t_fsync_no_projection (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, fsync_after_insert = 1, fsync_after_insert_each_part = 1;

CREATE TABLE t_fsync_projection (k UInt64, s String, PROJECTION p (SELECT k, s ORDER BY s)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, fsync_after_insert = 1, fsync_after_insert_each_part = 1;

INSERT INTO t_fsync_no_projection SELECT number, toString(number) FROM numbers(1000)
SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_block_size = 100000, async_insert = 0;

INSERT INTO t_fsync_projection SELECT number, toString(number) FROM numbers(1000)
SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_block_size = 100000, async_insert = 0;

SELECT count(), sum(k) FROM t_fsync_projection;
SELECT count() > 0 FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_fsync_projection' AND name = 'p' AND active;

SYSTEM FLUSH LOGS query_log;

-- The same part plus its projection: the files of the projection are synced in addition.
SELECT with_projection > without_projection
FROM
(
    SELECT
        (SELECT ProfileEvents['FileSync'] FROM system.query_log
         WHERE current_database = currentDatabase() AND type = 'QueryFinish'
           AND query LIKE 'INSERT INTO t_fsync_projection%'
         ORDER BY event_time_microseconds DESC LIMIT 1) AS with_projection,
        (SELECT ProfileEvents['FileSync'] FROM system.query_log
         WHERE current_database = currentDatabase() AND type = 'QueryFinish'
           AND query LIKE 'INSERT INTO t_fsync_no_projection%'
         ORDER BY event_time_microseconds DESC LIMIT 1) AS without_projection
);

DROP TABLE t_fsync_no_projection;
DROP TABLE t_fsync_projection;
