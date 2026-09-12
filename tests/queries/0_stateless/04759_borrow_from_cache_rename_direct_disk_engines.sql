-- Tags: no-fasttest, no-replicated-database, no-shared-catalog

-- Regression test: `RENAME TABLE` of the direct-disk engines (`Log`, `TinyLog`, `StripeLog`,
-- `Set`, `Join`) on an object-storage disk. `StorageLog::rename` used to pre-create the
-- destination directory before `moveDirectory`, which metadata storages of `DiskObjectStorage`
-- reject (`rename` semantics: the destination must not exist), and `StorageSetOrJoinBase::rename`
-- used `replaceFile`, which has file-only semantics on `DiskObjectStorage`. A rename that moves
-- table data only happens in an `Ordinary` database.

-- First, create a filesystem cache by making a cached disk.
DROP TABLE IF EXISTS tmp_cache_creator;
CREATE TABLE tmp_cache_creator (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS disk = disk(
    type = cache,
    disk = 'local_disk',
    name = '04759_cache_creator',
    path = '04759_borrow_test_cache/',
    max_size = '100Mi',
    load_metadata_asynchronously = 0
);

-- Register a named borrow disk. The direct-disk engines accept only a named disk (not an inline
-- definition), so the disk is introduced via a throwaway MergeTree table.
DROP TABLE IF EXISTS tmp_disk_creator;
CREATE TABLE tmp_disk_creator (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = 'borrow_from_cache',
    metadata_type = 'memory',
    cache_name = '04759_cache_creator',
    name = '04759_borrowed_disk'
);

SET allow_deprecated_database_ordinary = 1;
-- Suppress the deprecation warning emitted for the Ordinary database engine.
SET send_logs_level = 'fatal';
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE log_table (key UInt64, value String) ENGINE = Log SETTINGS disk = '04759_borrowed_disk';
INSERT INTO log_table VALUES (1, 'log');
RENAME TABLE log_table TO log_table_renamed;
SELECT * FROM log_table_renamed;

CREATE TABLE tiny_log_table (key UInt64, value String) ENGINE = TinyLog SETTINGS disk = '04759_borrowed_disk';
INSERT INTO tiny_log_table VALUES (2, 'tinylog');
RENAME TABLE tiny_log_table TO tiny_log_table_renamed;
SELECT * FROM tiny_log_table_renamed;

CREATE TABLE stripe_log_table (key UInt64, value String) ENGINE = StripeLog SETTINGS disk = '04759_borrowed_disk';
INSERT INTO stripe_log_table VALUES (3, 'stripelog');
RENAME TABLE stripe_log_table TO stripe_log_table_renamed;
SELECT * FROM stripe_log_table_renamed;

CREATE TABLE set_table (key UInt64) ENGINE = Set SETTINGS disk = '04759_borrowed_disk';
INSERT INTO set_table VALUES (4);
RENAME TABLE set_table TO set_table_renamed;
SELECT number FROM numbers(10) WHERE number IN set_table_renamed;

CREATE TABLE join_table (key UInt64, value String) ENGINE = Join(ANY, LEFT, key) SETTINGS disk = '04759_borrowed_disk';
INSERT INTO join_table VALUES (5, 'join');
RENAME TABLE join_table TO join_table_renamed;
SELECT joinGet(join_table_renamed, 'value', toUInt64(5));

-- The renamed tables must still be writable and readable after the move.
INSERT INTO log_table_renamed VALUES (6, 'log2');
SELECT count() FROM log_table_renamed;

DROP TABLE log_table_renamed;
DROP TABLE tiny_log_table_renamed;
DROP TABLE stripe_log_table_renamed;
DROP TABLE set_table_renamed;
DROP TABLE join_table_renamed;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

USE {CLICKHOUSE_DATABASE:Identifier};
DROP TABLE tmp_disk_creator;
DROP TABLE tmp_cache_creator;
