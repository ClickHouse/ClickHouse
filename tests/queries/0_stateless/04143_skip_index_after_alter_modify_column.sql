-- Test that skip indexes are properly handled when `ALTER MODIFY COLUMN` changes
-- the column type and the mutation has not yet been applied.
-- This verifies that `supportsSkipIndexesOnDataRead` disables the data-read phase
-- of skip-index application while there are pending alter mutations: in that phase
-- the index would be applied without the per-part `can_use_index` check, so the
-- on-disk index data (serialized with the old type) would be evaluated against the
-- new type and produce wrong results.
--
-- Uses UInt64 → Float64 conversion because both types use 8-byte fixed-width
-- serialization: the old UInt64 bytes get reinterpreted as Float64 without a
-- deserialization error, producing tiny denormalized values that silently cause
-- the set index to incorrectly skip granules (returning 0 rows instead of 128).
--
-- `max_rows_to_read = 0` is needed to enable the data-read phase in
-- `supportsSkipIndexesOnDataRead` (it disables itself when `read_overflow_mode = throw`
-- and `max_rows_to_read` is set, which `clickhouse-test` injects by default).

DROP TABLE IF EXISTS test_skip_index_alter;

CREATE TABLE test_skip_index_alter
(
    id UInt64,
    value UInt64,
    INDEX idx_value (value) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 128;

-- Insert enough rows across multiple granules so the skip index is populated and used.
-- Granule 0 (rows 0..127) has value 200, granule 1 (rows 128..255) has value 300.
-- A query for 300 should skip the first granule via the set index.
INSERT INTO test_skip_index_alter SELECT number, if(number < 128, 200, 300) FROM numbers(256);

-- Verify initial index usage works
SELECT count() FROM test_skip_index_alter WHERE value = 300 SETTINGS force_data_skipping_indices = 'idx_value', use_skip_indexes_on_data_read = 1, max_rows_to_read = 0;

-- Stop merges so the mutation doesn't get applied
SYSTEM STOP MERGES test_skip_index_alter;

-- Change column type; creates an ALTER mutation (READ_COLUMN), not a data mutation
SET alter_sync = 0, mutations_sync = 0;
ALTER TABLE test_skip_index_alter MODIFY COLUMN value Float64;

-- The index data is now incompatible with the new type.
-- Without the fix, the data-read phase reads the on-disk index (serialized as UInt64)
-- using the new Float64 deserialization, producing tiny denormalized values that don't
-- match 300.0, so the index incorrectly skips all granules and returns 0 rows.
-- With the fix, the data-read phase is disabled while alter mutations are pending,
-- and the primary-key analysis correctly excludes the incompatible index per part.
SELECT count() FROM test_skip_index_alter WHERE value = 300.0 SETTINGS force_data_skipping_indices = 'idx_value', use_skip_indexes_on_data_read = 1, max_rows_to_read = 0;

SYSTEM START MERGES test_skip_index_alter;
OPTIMIZE TABLE test_skip_index_alter FINAL SETTINGS mutations_sync = 2;

-- After mutation completes, the index should work with the new type
SELECT count() FROM test_skip_index_alter WHERE value = 300.0 SETTINGS force_data_skipping_indices = 'idx_value', use_skip_indexes_on_data_read = 1, max_rows_to_read = 0;

DROP TABLE test_skip_index_alter;
