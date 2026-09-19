-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS patch_merge_projection_04839;

-- Projection analogue of 04839_json_shared_regexp_patch_merge.sql: a merge applying a lightweight-
-- update patch must preserve SHARED REGEXP provenance for a projection too, not just the base part.
CREATE TABLE patch_merge_projection_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=10),
    PROJECTION p (SELECT id, j ORDER BY id)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    enable_block_number_column=1,
    enable_block_offset_column=1,
    apply_patches_on_merge=1,
    min_rows_for_wide_part=0,
    min_bytes_for_wide_part=0;

SYSTEM STOP MERGES patch_merge_projection_04839;

-- Keep two ordinary parts so OPTIMIZE must run MergeTask rather than rewriting a single part.
INSERT INTO patch_merge_projection_04839 VALUES (1, '{"keep":1}');
INSERT INTO patch_merge_projection_04839 VALUES (2, '{"keep":2}');

-- Only the patch part records the policy that placed `force_new` in shared data.
ALTER TABLE patch_merge_projection_04839
    MODIFY COLUMN j JSON(max_dynamic_paths=10, SHARED REGEXP '^force');

UPDATE patch_merge_projection_04839
SET j = '{"keep":1,"force_new":9}'
WHERE id=1
SETTINGS mutations_sync=2;

ALTER TABLE patch_merge_projection_04839 MODIFY COLUMN j JSON(max_dynamic_paths=10);

-- With `apply_patches_on_merge=1`, the rebuilt projection must union patch-part provenance too.
SYSTEM START MERGES patch_merge_projection_04839;
OPTIMIZE TABLE patch_merge_projection_04839 FINAL;

-- The regression: this must show the rule. Without threading patch provenance through the
-- merge-time projection rebuild, the projection's own type comes back bare.
SELECT
    'patch merge projection provenance',
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='patch_merge_projection_04839' AND column='j' AND active;

DROP TABLE patch_merge_projection_04839;
