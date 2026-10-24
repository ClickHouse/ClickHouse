-- Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage, no-async-insert, no-random-merge-tree-settings
-- no-fasttest: UNIQUE KEY needs RocksDB.
-- no-object-storage, no-s3-storage: object storage has no fsync, so FileSync stays 0 there.

-- With fsync_after_insert = 1 and fsync_after_insert_each_part = 0 the `unique_key_index.sst` of a
-- UNIQUE KEY part must not be synced when it is written: it is synced once, together with the other
-- files of the part, when the INSERT finishes.
--
-- A UNIQUE KEY part also has `txn_version.txt`, which the UNIQUE KEY transaction writes with its own
-- sync regardless of `fsync_after_insert`, and which is synced once more with the rest of the part.
-- Those writes are counted by an INSERT into the same UNIQUE KEY table with `fsync_after_insert = 0`
-- and subtracted. What remains on top of the same INSERT into a table without the UNIQUE KEY is
-- two fsyncs per part: `unique_key_index.sst` and `txn_version.txt`, once each.
--
-- Under load in CI a single INSERT occasionally gets one or two `FileSync` more or less (a few percent
-- of the runs in the flaky check): `txn_version.txt` is not always written, and synced, by the INSERT
-- itself. So every table gets five INSERTs and the median per table is compared, and only the upper
-- bound is checked. Syncing `unique_key_index.sst` both when it is written and at the end of the
-- INSERT would make it three.

SET enable_unique_key = 1;

DROP TABLE IF EXISTS t_fsync_uk;
DROP TABLE IF EXISTS t_fsync_uk_off;
DROP TABLE IF EXISTS t_fsync_no_uk;

CREATE TABLE t_fsync_uk (k UInt64, s String) ENGINE = MergeTree UNIQUE KEY (k) ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, fsync_after_insert = 1, fsync_after_insert_each_part = 0;

CREATE TABLE t_fsync_uk_off (k UInt64, s String) ENGINE = MergeTree UNIQUE KEY (k) ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, fsync_after_insert = 0;

CREATE TABLE t_fsync_no_uk (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, fsync_after_insert = 1, fsync_after_insert_each_part = 0;

-- Every INSERT must write one new part next to the same set of existing parts.
SYSTEM STOP MERGES t_fsync_uk;
SYSTEM STOP MERGES t_fsync_uk_off;
SYSTEM STOP MERGES t_fsync_no_uk;

INSERT INTO t_fsync_uk SELECT number, toString(number) FROM numbers(0, 100);
INSERT INTO t_fsync_uk SELECT number, toString(number) FROM numbers(100, 100);
INSERT INTO t_fsync_uk SELECT number, toString(number) FROM numbers(200, 100);
INSERT INTO t_fsync_uk SELECT number, toString(number) FROM numbers(300, 100);
INSERT INTO t_fsync_uk SELECT number, toString(number) FROM numbers(400, 100);
INSERT INTO t_fsync_uk_off SELECT number, toString(number) FROM numbers(0, 100);
INSERT INTO t_fsync_uk_off SELECT number, toString(number) FROM numbers(100, 100);
INSERT INTO t_fsync_uk_off SELECT number, toString(number) FROM numbers(200, 100);
INSERT INTO t_fsync_uk_off SELECT number, toString(number) FROM numbers(300, 100);
INSERT INTO t_fsync_uk_off SELECT number, toString(number) FROM numbers(400, 100);
INSERT INTO t_fsync_no_uk SELECT number, toString(number) FROM numbers(0, 100);
INSERT INTO t_fsync_no_uk SELECT number, toString(number) FROM numbers(100, 100);
INSERT INTO t_fsync_no_uk SELECT number, toString(number) FROM numbers(200, 100);
INSERT INTO t_fsync_no_uk SELECT number, toString(number) FROM numbers(300, 100);
INSERT INTO t_fsync_no_uk SELECT number, toString(number) FROM numbers(400, 100);

SELECT count(), sum(k) FROM t_fsync_uk;

SYSTEM FLUSH LOGS query_log;

SELECT
    ((SELECT medianExact(ProfileEvents['FileSync']) FROM system.query_log
     WHERE current_database = currentDatabase() AND type = 'QueryFinish'
       AND query LIKE 'INSERT INTO t_fsync_uk %')
    -
    (SELECT medianExact(ProfileEvents['FileSync']) FROM system.query_log
     WHERE current_database = currentDatabase() AND type = 'QueryFinish'
       AND query LIKE 'INSERT INTO t_fsync_uk_off %')
    -
    (SELECT medianExact(ProfileEvents['FileSync']) FROM system.query_log
     WHERE current_database = currentDatabase() AND type = 'QueryFinish'
       AND query LIKE 'INSERT INTO t_fsync_no_uk %')) <= 2;

DROP TABLE t_fsync_uk;
DROP TABLE t_fsync_uk_off;
DROP TABLE t_fsync_no_uk;
