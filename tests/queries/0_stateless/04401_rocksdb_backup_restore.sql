-- Tags: use-rocksdb, no-fasttest
-- Tag no-fasttest: rocksdb engine is not enabled in fasttest build (ENABLE_LIBRARIES=0)

-- EmbeddedRocksDB used to back up only table metadata, silently dropping all data on restore.
-- See https://github.com/ClickHouse/ClickHouse/issues/109213

DROP TABLE IF EXISTS 04401_rocksdb SYNC;

CREATE TABLE 04401_rocksdb (k UInt64, v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO 04401_rocksdb SELECT number, 'val_' || toString(number) FROM numbers(1000);

BACKUP TABLE 04401_rocksdb TO Memory('04401_rocksdb_backup') FORMAT Null;

DROP TABLE 04401_rocksdb SYNC;
RESTORE TABLE 04401_rocksdb FROM Memory('04401_rocksdb_backup') FORMAT Null;

-- Data must survive the round-trip.
SELECT count(), sum(k), sum(cityHash64(v)) FROM 04401_rocksdb;

-- Restoring into a non-empty table is rejected unless allow_non_empty_tables is set.
RESTORE TABLE 04401_rocksdb FROM Memory('04401_rocksdb_backup') FORMAT Null; -- { serverError CANNOT_RESTORE_TABLE }

DROP TABLE 04401_rocksdb SYNC;

-- Multi-column primary key and complex value types round-trip too (raw key/value bytes are copied verbatim).
DROP TABLE IF EXISTS 04401_rocksdb_mc SYNC;

CREATE TABLE 04401_rocksdb_mc (a UInt32, b String, v Nullable(String), arr Array(UInt32)) ENGINE = EmbeddedRocksDB PRIMARY KEY (a, b);
INSERT INTO 04401_rocksdb_mc VALUES (1, 'x', 'a', [1, 2, 3]), (2, 'y', NULL, []), (3, 'z', 'c', [9]);

BACKUP TABLE 04401_rocksdb_mc TO Memory('04401_rocksdb_mc_backup') FORMAT Null;

DROP TABLE 04401_rocksdb_mc SYNC;
RESTORE TABLE 04401_rocksdb_mc FROM Memory('04401_rocksdb_mc_backup') FORMAT Null;

SELECT a, b, v, arr FROM 04401_rocksdb_mc ORDER BY a, b;

-- An empty table backs up and restores without error.
DROP TABLE IF EXISTS 04401_rocksdb_empty SYNC;

CREATE TABLE 04401_rocksdb_empty (k UInt64, v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
BACKUP TABLE 04401_rocksdb_empty TO Memory('04401_rocksdb_empty_backup') FORMAT Null;
DROP TABLE 04401_rocksdb_empty SYNC;
RESTORE TABLE 04401_rocksdb_empty FROM Memory('04401_rocksdb_empty_backup') FORMAT Null;
SELECT count() FROM 04401_rocksdb_empty;

DROP TABLE 04401_rocksdb_mc SYNC;
DROP TABLE 04401_rocksdb_empty SYNC;

-- A backup that carries no data.bin (an old metadata-only backup from before this fix, or a
-- structure_only backup) must fail the data restore instead of silently recreating an empty table
-- (that silent empty restore is exactly the #109213 data loss). A structure_only backup is the
-- deterministic way to produce such a data-less backup here.
DROP TABLE IF EXISTS 04401_rocksdb_meta SYNC;

CREATE TABLE 04401_rocksdb_meta (k UInt64, v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO 04401_rocksdb_meta SELECT number, 'v_' || toString(number) FROM numbers(100);
BACKUP TABLE 04401_rocksdb_meta TO Memory('04401_rocksdb_meta_backup') SETTINGS structure_only = true FORMAT Null;
DROP TABLE 04401_rocksdb_meta SYNC;
RESTORE TABLE 04401_rocksdb_meta FROM Memory('04401_rocksdb_meta_backup') FORMAT Null; -- { serverError CANNOT_RESTORE_TABLE }
-- A structure_only restore of the same backup is still allowed (it does not touch data).
RESTORE TABLE 04401_rocksdb_meta FROM Memory('04401_rocksdb_meta_backup') SETTINGS structure_only = true FORMAT Null;
SELECT count() FROM 04401_rocksdb_meta;

DROP TABLE 04401_rocksdb_meta SYNC;

-- A TTL table preserves the original per-row expiration across a backup/restore.
-- The backup keeps the raw RocksDB value with its trailing creation timestamp (read/written via
-- GetRootDB so the DBWithTTL wrapper does not strip it on backup nor refresh it on restore).
-- The rows are inserted with ttl = 1s, so after a > 1s pause they are expired; OPTIMIZE runs the
-- TTL compaction filter and must drop them. If restore had reset the timestamps, the rows would
-- survive OPTIMIZE (the bug this guards against).
DROP TABLE IF EXISTS 04401_rocksdb_ttl SYNC;

CREATE TABLE 04401_rocksdb_ttl (k UInt64, v String) ENGINE = EmbeddedRocksDB(1) PRIMARY KEY k;
INSERT INTO 04401_rocksdb_ttl VALUES (1, 'a'), (2, 'b');
BACKUP TABLE 04401_rocksdb_ttl TO Memory('04401_rocksdb_ttl_backup') FORMAT Null;
SELECT sleep(3);
DROP TABLE 04401_rocksdb_ttl SYNC;
RESTORE TABLE 04401_rocksdb_ttl FROM Memory('04401_rocksdb_ttl_backup') FORMAT Null;
OPTIMIZE TABLE 04401_rocksdb_ttl;
-- Expired rows are dropped because their original timestamps were preserved.
SELECT count() FROM 04401_rocksdb_ttl;

DROP TABLE 04401_rocksdb_ttl SYNC;

-- A larger table restores correctly through the incremental WriteBatch flush (the restore no longer
-- buffers the whole table in a single batch). The raw payload here exceeds the flush threshold, so
-- more than one batch is written.
DROP TABLE IF EXISTS 04401_rocksdb_big SYNC;

CREATE TABLE 04401_rocksdb_big (k UInt64, v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO 04401_rocksdb_big SELECT number, repeat('x', 500) FROM numbers(200000);
BACKUP TABLE 04401_rocksdb_big TO Memory('04401_rocksdb_big_backup') FORMAT Null;
DROP TABLE 04401_rocksdb_big SYNC;
RESTORE TABLE 04401_rocksdb_big FROM Memory('04401_rocksdb_big_backup') FORMAT Null;
SELECT count(), sum(k) FROM 04401_rocksdb_big;

DROP TABLE 04401_rocksdb_big SYNC;
