SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_bi_a SYNC;
DROP TABLE IF EXISTS t_bi_b SYNC;
DROP TABLE IF EXISTS t_bi_c SYNC;
DROP TABLE IF EXISTS t_bi_d SYNC;
DROP TABLE IF EXISTS t_bi_e SYNC;
DROP TABLE IF EXISTS t_bi_f SYNC;
DROP TABLE IF EXISTS t_bi_g SYNC;
DROP TABLE IF EXISTS t_bi_j SYNC;

-- A, H, I: `enable_block_number_column` is off while `enable_block_offset_column` is on across the
-- merge, so the merged part must still store both columns and keep one address per row.
-- `max_bytes_to_merge_at_max_space_in_pool = 1` keeps background merges away from the two inserted
-- parts; the explicit OPTIMIZE FINAL below ignores that limit.
CREATE TABLE t_bi_a (id UInt64, v String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         apply_patches_on_merge = 0, patch_parts_version = 'v2',
         max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_bi_a VALUES (1, 'a');
INSERT INTO t_bi_a VALUES (2, 'b');
ALTER TABLE t_bi_a MODIFY SETTING enable_block_number_column = 0;
OPTIMIZE TABLE t_bi_a FINAL;
ALTER TABLE t_bi_a MODIFY SETTING enable_block_number_column = 1;

SELECT 'H stored columns', groupArray(column) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_bi_a' AND active;

SELECT 'I address', id, _block_number, _block_offset FROM t_bi_a ORDER BY id;

UPDATE t_bi_a SET v = 'u' WHERE id = 1;
SELECT 'A patch applied on read', id, v FROM t_bi_a ORDER BY id;

-- B: the same identity, with the patch applied by a merge instead of on the fly.
CREATE TABLE t_bi_b (id UInt64, v String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         apply_patches_on_merge = 1, patch_parts_version = 'v2',
         max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_bi_b VALUES (1, 'a');
INSERT INTO t_bi_b VALUES (2, 'b');
ALTER TABLE t_bi_b MODIFY SETTING enable_block_number_column = 0;
OPTIMIZE TABLE t_bi_b FINAL SETTINGS optimize_throw_if_noop = 1;
ALTER TABLE t_bi_b MODIFY SETTING enable_block_number_column = 1;

UPDATE t_bi_b SET v = 'u' WHERE id = 1;
OPTIMIZE TABLE t_bi_b FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'B patch applied on merge', id, v FROM t_bi_b ORDER BY id SETTINGS apply_patch_parts = 0;
SELECT 'B patch on read too', id, v FROM t_bi_b ORDER BY id SETTINGS apply_patch_parts = 1;

-- C: a non-empty sorting key that does not separate the two rows, so the sorting-key prefix of the
-- v2 patch identity cannot substitute for `_block_number`.
CREATE TABLE t_bi_c (id UInt64, v String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         apply_patches_on_merge = 0, patch_parts_version = 'v2',
         max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_bi_c VALUES (1, 'a');
INSERT INTO t_bi_c VALUES (1, 'b');
ALTER TABLE t_bi_c MODIFY SETTING enable_block_number_column = 0;
OPTIMIZE TABLE t_bi_c FINAL SETTINGS optimize_throw_if_noop = 1;
ALTER TABLE t_bi_c MODIFY SETTING enable_block_number_column = 1;

UPDATE t_bi_c SET v = 'u' WHERE v = 'a';
SELECT 'C duplicate sorting key', id, v FROM t_bi_c ORDER BY v;

-- D: the v1 patch format keys a row on the same pair. Rewriting the base part after the update is
-- what makes the read consult that pair; a shared address there is a LOGICAL_ERROR in debug and
-- sanitizer builds rather than a wrong row.
CREATE TABLE t_bi_d (id UInt64, v String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         apply_patches_on_merge = 0, patch_parts_version = 'v1',
         max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_bi_d VALUES (1, 'a');
INSERT INTO t_bi_d VALUES (2, 'b');
ALTER TABLE t_bi_d MODIFY SETTING enable_block_number_column = 0;
OPTIMIZE TABLE t_bi_d FINAL SETTINGS optimize_throw_if_noop = 1;
ALTER TABLE t_bi_d MODIFY SETTING enable_block_number_column = 1;

UPDATE t_bi_d SET v = 'u' WHERE id = 1;
OPTIMIZE TABLE t_bi_d FINAL SETTINGS optimize_throw_if_noop = 1;
SELECT 'D v1 patch format', id, v FROM t_bi_d ORDER BY id;

-- E, F: the same two fixtures without the toggle. Both columns are materialized throughout, so the
-- address is unique and the update is correct: what the other arms exercise is the asymmetry, not a
-- non-unique sorting key.
CREATE TABLE t_bi_e (id UInt64, v String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         apply_patches_on_merge = 0, patch_parts_version = 'v2',
         max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_bi_e VALUES (1, 'a');
INSERT INTO t_bi_e VALUES (2, 'b');
OPTIMIZE TABLE t_bi_e FINAL;
UPDATE t_bi_e SET v = 'u' WHERE id = 1;
SELECT 'E control, no toggle', id, v FROM t_bi_e ORDER BY id;

CREATE TABLE t_bi_f (id UInt64, v String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         apply_patches_on_merge = 0, patch_parts_version = 'v2',
         max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_bi_f VALUES (1, 'a');
INSERT INTO t_bi_f VALUES (1, 'b');
OPTIMIZE TABLE t_bi_f FINAL;
UPDATE t_bi_f SET v = 'u' WHERE v = 'a';
SELECT 'F control, duplicate key without toggle', id, v FROM t_bi_f ORDER BY v;

-- G: the reported fixture. A sorting key that separates the rows already made the v2 identity unique,
-- so this one is a guard against losing that, not a reproduction of the defect.
CREATE TABLE t_bi_g (id UInt64, v String) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         apply_patches_on_merge = 0, patch_parts_version = 'v2',
         max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_bi_g VALUES (1, 'a');
INSERT INTO t_bi_g VALUES (2, 'b');
ALTER TABLE t_bi_g MODIFY SETTING enable_block_number_column = 0;
OPTIMIZE TABLE t_bi_g FINAL SETTINGS optimize_throw_if_noop = 1;
ALTER TABLE t_bi_g MODIFY SETTING enable_block_number_column = 1;

UPDATE t_bi_g SET v = 'u' WHERE id = 1;
SELECT 'G reported fixture', id, v FROM t_bi_g ORDER BY id;

-- J: a mutation materializes `_block_offset` as `_part_offset`, which is unique inside the part, so
-- the mutation path cannot produce a shared address and stores `_block_offset` alone unchanged.
CREATE TABLE t_bi_j (id UInt64, v String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 0,
         apply_patches_on_merge = 0, patch_parts_version = 'v2',
         max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_bi_j VALUES (1, 'a');
INSERT INTO t_bi_j VALUES (2, 'b');
OPTIMIZE TABLE t_bi_j FINAL;
ALTER TABLE t_bi_j MODIFY SETTING enable_block_offset_column = 1;
ALTER TABLE t_bi_j UPDATE v = upper(v) WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'J mutated columns', groupArray(column) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_bi_j' AND active;

SELECT 'J address', id, _block_number, _block_offset FROM t_bi_j ORDER BY id;

ALTER TABLE t_bi_j MODIFY SETTING enable_block_number_column = 1;
UPDATE t_bi_j SET v = 'u' WHERE id = 1;
SELECT 'J control, mutation path', id, v FROM t_bi_j ORDER BY id;

DROP TABLE t_bi_a;
DROP TABLE t_bi_b;
DROP TABLE t_bi_c;
DROP TABLE t_bi_d;
DROP TABLE t_bi_e;
DROP TABLE t_bi_f;
DROP TABLE t_bi_g;
DROP TABLE t_bi_j;
