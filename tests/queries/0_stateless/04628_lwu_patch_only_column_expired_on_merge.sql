SET enable_lightweight_update = 1;

-- Phase 1: horizontal merge.
-- A no-default column whose only live value lives in a patch part (never materialized into a
-- data part) must survive OPTIMIZE FINAL: the merged part claims the patch version, so expiring
-- the column would silently drop the patch data.
DROP TABLE IF EXISTS t_patch_only_expire_h;
CREATE TABLE t_patch_only_expire_h (id UInt64, value String)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         apply_patches_on_merge = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_patch_only_expire_h VALUES (1, 'a'), (2, 'b');
INSERT INTO t_patch_only_expire_h VALUES (3, 'c');

ALTER TABLE t_patch_only_expire_h ADD COLUMN acol Nullable(Int64);
UPDATE t_patch_only_expire_h SET acol = 42 WHERE id = 1;

SELECT 'h_before', id, acol FROM t_patch_only_expire_h ORDER BY id;

OPTIMIZE TABLE t_patch_only_expire_h FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT 'h_after', id, acol FROM t_patch_only_expire_h ORDER BY id;
-- The merged data part (not the patch part) must physically carry acol.
SELECT 'h_nonpatch_acol_parts', count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_patch_only_expire_h'
  AND active AND column = 'acol' AND part_name NOT LIKE 'patch-%';

DROP TABLE t_patch_only_expire_h;

-- Phase 2: same scenario forced onto the Vertical merge algorithm (gather path).
DROP TABLE IF EXISTS t_patch_only_expire_v;
CREATE TABLE t_patch_only_expire_v (id UInt64, value String)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         apply_patches_on_merge = 1, min_bytes_for_wide_part = 0,
         vertical_merge_algorithm_min_rows_to_activate = 1,
         vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO t_patch_only_expire_v VALUES (1, 'a'), (2, 'b');
INSERT INTO t_patch_only_expire_v VALUES (3, 'c');

ALTER TABLE t_patch_only_expire_v ADD COLUMN acol Nullable(Int64);
UPDATE t_patch_only_expire_v SET acol = 42 WHERE id = 1;

SELECT 'v_before', id, acol FROM t_patch_only_expire_v ORDER BY id;

OPTIMIZE TABLE t_patch_only_expire_v FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT 'v_after', id, acol FROM t_patch_only_expire_v ORDER BY id;
SELECT 'v_nonpatch_acol_parts', count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_patch_only_expire_v'
  AND active AND column = 'acol' AND part_name NOT LIKE 'patch-%';

DROP TABLE t_patch_only_expire_v;
