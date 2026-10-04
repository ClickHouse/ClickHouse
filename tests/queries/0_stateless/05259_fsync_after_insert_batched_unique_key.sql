-- Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage, no-async-insert, no-random-merge-tree-settings
-- no-fasttest: UNIQUE KEY needs RocksDB.
-- no-object-storage, no-s3-storage: object storage has no fsync, so FileSync stays 0 there.

-- With fsync_after_insert = 1 and fsync_after_insert_each_part = 0 the `unique_key_index.sst` of a
-- UNIQUE KEY part must not be synced when it is written: it is synced once, together with the other
-- files of the part, when the INSERT finishes. So the INSERT into a UNIQUE KEY table does exactly
-- one more fsync per part than the same INSERT into the same table without the UNIQUE KEY.

SET enable_unique_key = 1;

DROP TABLE IF EXISTS t_fsync_uk;
DROP TABLE IF EXISTS t_fsync_no_uk;

CREATE TABLE t_fsync_uk (k UInt64, s String) ENGINE = MergeTree UNIQUE KEY (k) ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, fsync_after_insert = 1, fsync_after_insert_each_part = 0;

CREATE TABLE t_fsync_no_uk (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, fsync_after_insert = 1, fsync_after_insert_each_part = 0;

INSERT INTO t_fsync_uk SELECT number, toString(number) FROM numbers(100);
INSERT INTO t_fsync_no_uk SELECT number, toString(number) FROM numbers(100);

SELECT count(), sum(k) FROM t_fsync_uk;

SYSTEM FLUSH LOGS query_log;

SELECT
    (SELECT ProfileEvents['FileSync'] FROM system.query_log
     WHERE current_database = currentDatabase() AND type = 'QueryFinish'
       AND query LIKE 'INSERT INTO t_fsync_uk%'
     ORDER BY event_time_microseconds DESC LIMIT 1)
    -
    (SELECT ProfileEvents['FileSync'] FROM system.query_log
     WHERE current_database = currentDatabase() AND type = 'QueryFinish'
       AND query LIKE 'INSERT INTO t_fsync_no_uk%'
     ORDER BY event_time_microseconds DESC LIMIT 1);

DROP TABLE t_fsync_uk;
DROP TABLE t_fsync_no_uk;
