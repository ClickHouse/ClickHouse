-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS patch_merge_projection_member_04839;

-- Member-qualified sibling of 04839_json_shared_regexp_patch_merge_projection.sql: when the patch
-- part is the only provenance carrier, the dotted-candidate fallback must also serve a projection
-- that reads the JSON through tupleElement() rather than as a plain column.
CREATE TABLE patch_merge_projection_member_04839
(
    id UInt64,
    t Tuple(s String, doc JSON(max_dynamic_paths=10)),
    PROJECTION p (SELECT id, tupleElement(t, 2) AS doc ORDER BY id)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    enable_block_number_column=1,
    enable_block_offset_column=1,
    apply_patches_on_merge=1,
    min_rows_for_wide_part=0,
    min_bytes_for_wide_part=0;

SYSTEM STOP MERGES patch_merge_projection_member_04839;

-- Keep two ordinary parts so OPTIMIZE must run MergeTask rather than rewriting a single part.
INSERT INTO patch_merge_projection_member_04839 VALUES (1, ('a', '{"keep":1}'));
INSERT INTO patch_merge_projection_member_04839 VALUES (2, ('b', '{"keep":2}'));

-- Only the patch part records the policy that placed `force_new` in shared data.
ALTER TABLE patch_merge_projection_member_04839
    MODIFY COLUMN t Tuple(s String, doc JSON(max_dynamic_paths=10, SHARED REGEXP '^force'));

UPDATE patch_merge_projection_member_04839
SET t = ('a', '{"keep":1,"force_new":9}')
WHERE id=1
SETTINGS mutations_sync=2;

ALTER TABLE patch_merge_projection_member_04839 MODIFY COLUMN t Tuple(s String, doc JSON(max_dynamic_paths=10));

-- With `apply_patches_on_merge=1`, the member-qualified donor must be found in the patch part.
SYSTEM START MERGES patch_merge_projection_member_04839;
OPTIMIZE TABLE patch_merge_projection_member_04839 FINAL;

SELECT
    'patch merge projection member provenance',
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='patch_merge_projection_member_04839' AND name='p' AND column != 'id' AND active;

DROP TABLE patch_merge_projection_member_04839;
