-- Tags: no-object-storage, no-random-merge-tree-settings
-- no-object-storage: object storage has no fsync, so FileSync stays 0 there.

-- With fsync_after_insert = 1 and fsync_after_insert_each_part = 0 (the default) an INSERT does
-- not fsync each part as it is written; it fsyncs the active parts covering the inserted data
-- once, when the query finishes. The fsyncs must still be accounted to the INSERT itself, i.e.
-- they have to happen before the query completes rather than in the background.

DROP TABLE IF EXISTS t_fsync_batched;
DROP TABLE IF EXISTS t_fsync_each_part;

CREATE TABLE t_fsync_batched (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, fsync_after_insert = 1, fsync_after_insert_each_part = 0;

CREATE TABLE t_fsync_each_part (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, fsync_after_insert = 1, fsync_after_insert_each_part = 1;

-- Keep every part written by the INSERT active, so the covering part of each of them is the part
-- itself and both modes have to sync the same set of parts.
SYSTEM STOP MERGES t_fsync_batched;
SYSTEM STOP MERGES t_fsync_each_part;

INSERT INTO t_fsync_batched SELECT number, toString(number) FROM numbers(1000)
SETTINGS max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0;

INSERT INTO t_fsync_each_part SELECT number, toString(number) FROM numbers(1000)
SETTINGS max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0;

SELECT count(), sum(k) FROM t_fsync_batched;
SELECT count(), sum(k) FROM t_fsync_each_part;

-- The same data ends up in the same number of parts either way.
SELECT
    (SELECT count() FROM system.parts
     WHERE database = currentDatabase() AND table = 't_fsync_batched' AND active)
    =
    (SELECT count() FROM system.parts
     WHERE database = currentDatabase() AND table = 't_fsync_each_part' AND active);

SYSTEM FLUSH LOGS query_log;

-- Both INSERTs fsync inside the query, not in the background afterwards. In the batched mode
-- every part written by the query still gets synced, so there is at least one fsync per part.
SELECT
    each_part > 0,
    batched >= parts
FROM
(
    SELECT
        (SELECT ProfileEvents['FileSync'] FROM system.query_log
         WHERE current_database = currentDatabase() AND type = 'QueryFinish'
           AND query LIKE 'INSERT INTO t_fsync_batched%'
         ORDER BY event_time_microseconds DESC LIMIT 1) AS batched,
        (SELECT ProfileEvents['FileSync'] FROM system.query_log
         WHERE current_database = currentDatabase() AND type = 'QueryFinish'
           AND query LIKE 'INSERT INTO t_fsync_each_part%'
         ORDER BY event_time_microseconds DESC LIMIT 1) AS each_part,
        (SELECT count() FROM system.parts
         WHERE database = currentDatabase() AND table = 't_fsync_batched' AND active) AS parts
);

DROP TABLE t_fsync_batched;
DROP TABLE t_fsync_each_part;

-- A lightweight UPDATE writes a patch part through a sink of its own. It is not synced when it is
-- finalized either, so the update has to fsync it before it is acknowledged, exactly like an INSERT.

DROP TABLE IF EXISTS t_fsync_patch;

CREATE TABLE t_fsync_patch (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, fsync_after_insert = 1, fsync_after_insert_each_part = 0,
    enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_fsync_patch SELECT number, number FROM numbers(100);

SET enable_lightweight_update = 1;
UPDATE t_fsync_patch SET v = v * 10 WHERE k % 2 = 0;

SELECT count(), sum(v) FROM t_fsync_patch;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['FileSync'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND query LIKE 'UPDATE t_fsync_patch%'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_fsync_patch;
