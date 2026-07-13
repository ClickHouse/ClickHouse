-- Tags: no-ordinary-database, no-fasttest, use-rocksdb
-- Tag no-ordinary-database: Sometimes cannot lock file most likely due to concurrent or adjacent tests, but we don't care how it works in Ordinary database
-- Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so rocksdb engine is not enabled by default

DROP TABLE IF EXISTS test_rocksdb_virtuals;

CREATE TABLE test_rocksdb_virtuals (key UInt64, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY key;

INSERT INTO test_rocksdb_virtuals VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- Check that _table virtual column is available
SELECT _table FROM test_rocksdb_virtuals ORDER BY key LIMIT 1;

-- Check that _table returns the correct table name
SELECT key, _table FROM test_rocksdb_virtuals ORDER BY key;

-- Check DESCRIBE shows _table as virtual column
DESCRIBE TABLE test_rocksdb_virtuals SETTINGS describe_include_virtual_columns = 1, describe_compact_output = 1;

-- Check that _table works with key lookup
SELECT _table FROM test_rocksdb_virtuals WHERE key = 2;

-- Check that selecting only _table works (no physical columns)
SELECT _table FROM test_rocksdb_virtuals LIMIT 1;

DROP TABLE test_rocksdb_virtuals;
