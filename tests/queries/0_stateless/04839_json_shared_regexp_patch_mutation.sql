-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS patch_mutation_04839;

CREATE TABLE patch_mutation_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=10)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    enable_block_number_column=1,
    enable_block_offset_column=1,
    apply_patches_on_merge=0,
    min_rows_for_wide_part=0,
    min_bytes_for_wide_part=0;

INSERT INTO patch_mutation_04839 VALUES (1, '{"keep":1}');

-- The base part predates the policy. Only the lightweight-update patch part records that
-- `force_new` was deliberately put in shared data.
ALTER TABLE patch_mutation_04839
    MODIFY COLUMN j JSON(max_dynamic_paths=10, SHARED REGEXP '^force');

UPDATE patch_mutation_04839
SET j = '{"keep":1,"force_new":7}'
WHERE id=1
SETTINGS mutations_sync=2;

ALTER TABLE patch_mutation_04839 MODIFY COLUMN j JSON(max_dynamic_paths=10);

-- Materializing the patch is a mutation. It must retain policy provenance from the patch even
-- though neither the current table metadata nor the base part contains the old rule.
ALTER TABLE patch_mutation_04839 APPLY PATCHES SETTINGS mutations_sync=2;

DETACH TABLE patch_mutation_04839;
ATTACH TABLE patch_mutation_04839;

SELECT
    'patch mutation provenance',
    count(),
    countIf(position(p.type, 'SHARED REGEXP') > 0)
FROM system.parts_columns AS p
WHERE p.database=currentDatabase() AND p.table='patch_mutation_04839' AND p.column='j' AND p.active
  AND NOT startsWith(p.partition_id, 'patch-');

SELECT
    'patch mutation placement',
    JSONDynamicPaths(j),
    JSONSharedDataPaths(j),
    j.force_new::UInt64
FROM patch_mutation_04839
SETTINGS apply_patch_parts=0;

DROP TABLE patch_mutation_04839;
