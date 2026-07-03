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
