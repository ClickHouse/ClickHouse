-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings: the test pins apply_mutations_on_fly / apply_patch_parts and the part layout on purpose.

DROP TABLE IF EXISTS t_skip_index_alter_nullable;

CREATE TABLE t_skip_index_alter_nullable
(
    id UInt64,
    value String,
    INDEX idx_value (value) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY id
PARTITION BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_skip_index_alter_nullable VALUES (1, '10'), (2, '20'), (3, '300');

-- Stop merges so the ALTER MODIFY COLUMN mutation stays pending and the old parts keep
-- their String-serialized set index data.
SYSTEM STOP MERGES t_skip_index_alter_nullable;

SET alter_sync = 0, mutations_sync = 0;
ALTER TABLE t_skip_index_alter_nullable MODIFY COLUMN value Nullable(UInt64);

-- With apply_mutations_on_fly = 0 AND apply_patch_parts = 0 the read snapshot used to omit the
-- pending READ_COLUMN alter mutation, so index analysis did not skip idx_value and read the old
-- (String-serialized) granule using the new Nullable(UInt64) type, raising a LOGICAL_ERROR
-- exception ("Sizes of nested column and null map ... are not equal after deserialization").
-- The query goes through the implicit count() projection (optimizeUseAggregateProjections) which
-- requests exact ranges and triggers the index read during planning.
SELECT count()
FROM t_skip_index_alter_nullable
WHERE value = 300
SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0, optimize_use_implicit_projections = 1, use_statistics_for_part_pruning = 0, enable_analyzer = 1;

-- Same query with the on-fly apply flags at their defaults: must also work and give the same result.
SELECT count()
FROM t_skip_index_alter_nullable
WHERE value = 300
SETTINGS optimize_use_implicit_projections = 1, use_statistics_for_part_pruning = 0, enable_analyzer = 1;

-- The same `need_alter_mutations` gate also feeds `supportsSkipIndexesOnDataRead`, so pin the
-- direct skip-index read as well: without the fix this throws in MergeTreeSkipIndexReader::read
-- (via MergeTreeIndexBulkGranulesSet::deserializeBinary), not during planning. `max_rows_to_read = 0`
-- is required because the data-read phase disables itself when clickhouse-test injects
-- `read_overflow_mode = throw` with a row limit. `use_skip_indexes = 1` is pinned explicitly:
-- `force_data_skipping_indices` does not throw when skip indexes are switched off globally, so
-- without the pin this row would still return the right value while exercising nothing.
SELECT id
FROM t_skip_index_alter_nullable
WHERE value = 300
SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0, use_skip_indexes = 1, force_data_skipping_indices = 'idx_value', use_skip_indexes_on_data_read = 1, max_rows_to_read = 0, optimize_use_implicit_projections = 0, use_statistics_for_part_pruning = 0, enable_analyzer = 1;

SYSTEM START MERGES t_skip_index_alter_nullable;
OPTIMIZE TABLE t_skip_index_alter_nullable FINAL SETTINGS mutations_sync = 2;

-- After the mutation is materialized the index is rebuilt for the new type and the result is unchanged.
SELECT count()
FROM t_skip_index_alter_nullable
WHERE value = 300
SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0, optimize_use_implicit_projections = 1, use_statistics_for_part_pruning = 0, enable_analyzer = 1;

DROP TABLE t_skip_index_alter_nullable;
