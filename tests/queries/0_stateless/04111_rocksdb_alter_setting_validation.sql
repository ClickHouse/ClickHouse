-- Tags: no-fasttest
-- Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so rocksdb engine is not enabled by default

-- Regression test for a latent `EmbeddedRocksDB` bug where `ALTER TABLE ... MODIFY SETTING`
-- with an invalid value (e.g. a non-boolean string for a `Bool` setting) would be persisted
-- to the table metadata on disk before the value was validated. The validation would throw,
-- but the metadata on disk was already corrupted, making the server unable to attach the
-- table on the next restart with `CANNOT_PARSE_BOOL` (or similar) errors. See #88443.
--
-- The fix validates setting values in `checkAlterIsPossible`, i.e. before any metadata is
-- written. After a failed `ALTER ... MODIFY SETTING` the table metadata must remain
-- unchanged and the subsequent valid ALTER must still succeed.

DROP TABLE IF EXISTS t_rocksdb_alter_validation;

CREATE TABLE t_rocksdb_alter_validation (key UInt64, value UInt64)
ENGINE = EmbeddedRocksDB PRIMARY KEY key;

-- Invalid Bool value: must throw and must NOT modify on-disk metadata
ALTER TABLE t_rocksdb_alter_validation MODIFY SETTING optimize_for_bulk_insert = '2020-02-29'; -- { serverError CANNOT_PARSE_BOOL }
SELECT 'after-invalid-bool:', create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 't_rocksdb_alter_validation';

-- Another invalid Bool value
ALTER TABLE t_rocksdb_alter_validation MODIFY SETTING optimize_for_bulk_insert = 'abc'; -- { serverError CANNOT_PARSE_BOOL }
SELECT 'after-invalid-bool2:', create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 't_rocksdb_alter_validation';

-- Invalid UInt64 value (non-numeric string): must throw and must NOT modify on-disk metadata
ALTER TABLE t_rocksdb_alter_validation MODIFY SETTING bulk_insert_block_size = 'not-a-number'; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT 'after-invalid-uint-string:', create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 't_rocksdb_alter_validation';

-- Invalid UInt64 value (out of range of unsigned: negative): must throw and must NOT modify on-disk metadata.
-- This reproduces the failure mode from #88443 (`Field value ... is out of range of ... type`).
ALTER TABLE t_rocksdb_alter_validation MODIFY SETTING bulk_insert_block_size = -5; -- { serverError CANNOT_CONVERT_TYPE }
SELECT 'after-invalid-uint-negative:', create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 't_rocksdb_alter_validation';

-- Valid ALTER still works
ALTER TABLE t_rocksdb_alter_validation MODIFY SETTING optimize_for_bulk_insert = 0;
SELECT 'after-valid-bool:', create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 't_rocksdb_alter_validation';

-- Valid UInt64 setting still works
ALTER TABLE t_rocksdb_alter_validation MODIFY SETTING bulk_insert_block_size = 1234;
SELECT 'after-valid-uint:', create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 't_rocksdb_alter_validation';

-- RESET SETTING still works
ALTER TABLE t_rocksdb_alter_validation RESET SETTING optimize_for_bulk_insert;
SELECT 'after-reset:', create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 't_rocksdb_alter_validation';

DROP TABLE t_rocksdb_alter_validation;
