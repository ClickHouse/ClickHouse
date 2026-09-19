-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS patch_merge_04839;

CREATE TABLE patch_merge_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=10)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    enable_block_number_column=1,
    enable_block_offset_column=1,
    apply_patches_on_merge=1,
    min_rows_for_wide_part=0,
    min_bytes_for_wide_part=0;

SYSTEM STOP MERGES patch_merge_04839;

-- Keep two ordinary parts so OPTIMIZE must run MergeTask rather than rewriting a single part.
INSERT INTO patch_merge_04839 VALUES (1, '{"keep":1}');
INSERT INTO patch_merge_04839 VALUES (2, '{"keep":2}');

-- Only the patch part records the policy that placed `force_new` in shared data.
ALTER TABLE patch_merge_04839
    MODIFY COLUMN j JSON(max_dynamic_paths=10, SHARED REGEXP '^force');

UPDATE patch_merge_04839
SET j = '{"keep":1,"force_new":9}'
WHERE id=1
SETTINGS mutations_sync=2;

ALTER TABLE patch_merge_04839 MODIFY COLUMN j JSON(max_dynamic_paths=10);

-- With `apply_patches_on_merge=1`, `MergeTask` must union patch-part provenance into the result type.
SYSTEM START MERGES patch_merge_04839;
OPTIMIZE TABLE patch_merge_04839 FINAL;

DETACH TABLE patch_merge_04839;
ATTACH TABLE patch_merge_04839;

SELECT
    'patch merge provenance',
    count(),
    countIf(position(p.type, 'SHARED REGEXP') > 0)
FROM system.parts_columns AS p
WHERE p.database=currentDatabase() AND p.table='patch_merge_04839' AND p.column='j' AND p.active
  AND NOT startsWith(p.partition_id, 'patch-');

-- Disabling patch reads proves the value was materialized by the merge itself.
SELECT
    'patch merge placement',
    id,
    JSONDynamicPaths(j),
    JSONSharedDataPaths(j),
    j.force_new::UInt64
FROM patch_merge_04839
WHERE id=1
SETTINGS apply_patch_parts=0;

DROP TABLE patch_merge_04839;
