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
-- (String-serialized) granule using the new Nullable(UInt64) type, aborting the server with a
-- LOGICAL_ERROR ("Sizes of nested column and null map ... are not equal after deserialization").
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

SYSTEM START MERGES t_skip_index_alter_nullable;
OPTIMIZE TABLE t_skip_index_alter_nullable FINAL SETTINGS mutations_sync = 2;

-- After the mutation is materialized the index is rebuilt for the new type and the result is unchanged.
SELECT count()
FROM t_skip_index_alter_nullable
WHERE value = 300
SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0, optimize_use_implicit_projections = 1, use_statistics_for_part_pruning = 0, enable_analyzer = 1;

DROP TABLE t_skip_index_alter_nullable;
