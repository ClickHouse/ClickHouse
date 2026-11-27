-- Tags: no-fasttest, use-rocksdb
-- Test: exercises `StorageEmbeddedRocksDB::optimize` rejection branches at
--   src/Storages/RocksDB/StorageEmbeddedRocksDB.cpp:430 (PARTITION)
--   src/Storages/RocksDB/StorageEmbeddedRocksDB.cpp:433 (FINAL)
--   src/Storages/RocksDB/StorageEmbeddedRocksDB.cpp:436 (DEDUPLICATE)
--   src/Storages/RocksDB/StorageEmbeddedRocksDB.cpp:439 (CLEANUP)
-- These NOT_IMPLEMENTED rejection paths are untested in the entire suite (LLVM-confirmed for line 439).

DROP TABLE IF EXISTS test_rocksdb_optimize_modifiers;

CREATE TABLE test_rocksdb_optimize_modifiers (key UInt64, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO test_rocksdb_optimize_modifiers SELECT number, toString(number) FROM numbers(10);

-- Plain OPTIMIZE is allowed (calls CompactRange).
OPTIMIZE TABLE test_rocksdb_optimize_modifiers;

-- Each OPTIMIZE modifier must be rejected with NOT_IMPLEMENTED (error code 48).
OPTIMIZE TABLE test_rocksdb_optimize_modifiers PARTITION ID 'all'; -- { serverError NOT_IMPLEMENTED }
OPTIMIZE TABLE test_rocksdb_optimize_modifiers FINAL; -- { serverError NOT_IMPLEMENTED }
OPTIMIZE TABLE test_rocksdb_optimize_modifiers DEDUPLICATE; -- { serverError NOT_IMPLEMENTED }
OPTIMIZE TABLE test_rocksdb_optimize_modifiers CLEANUP; -- { serverError NOT_IMPLEMENTED }

-- Sanity: the table is still usable after the rejected OPTIMIZE attempts.
SELECT count() FROM test_rocksdb_optimize_modifiers;

DROP TABLE test_rocksdb_optimize_modifiers;
