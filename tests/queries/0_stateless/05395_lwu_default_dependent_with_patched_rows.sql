-- Lightweight `UPDATE`s overwrite some rows of a `DEFAULT` column and some rows of a `DEFAULT` column that reads
-- it, neither stored in the part. After a later lightweight `UPDATE` of the column they read, the rows still
-- holding their `DEFAULT` follow it, computed from the patched rows of the column they read, regardless of
-- which rows are read together in one block.

DROP TABLE IF EXISTS t_lwu_dependent_patched;

SELECT 'v1';

CREATE TABLE t_lwu_dependent_patched (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, index_granularity = 1, patch_parts_version = 'v1';

INSERT INTO t_lwu_dependent_patched SELECT number FROM numbers(3);
ALTER TABLE t_lwu_dependent_patched ADD COLUMN a UInt32 DEFAULT 0;
ALTER TABLE t_lwu_dependent_patched ADD COLUMN z UInt32 DEFAULT a + 100;
ALTER TABLE t_lwu_dependent_patched ADD COLUMN y UInt32 DEFAULT z + 1;

UPDATE t_lwu_dependent_patched SET z = 999 WHERE id = 0;
UPDATE t_lwu_dependent_patched SET y = 777 WHERE id = 1;
UPDATE t_lwu_dependent_patched SET a = 5 WHERE 1;

SELECT id, a, z, y FROM t_lwu_dependent_patched ORDER BY id SETTINGS max_threads = 1, max_block_size = 100;
SELECT id, a, z, y FROM t_lwu_dependent_patched ORDER BY id SETTINGS max_threads = 1, max_block_size = 1;

DROP TABLE t_lwu_dependent_patched;

SELECT 'v2';

CREATE TABLE t_lwu_dependent_patched (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, index_granularity = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_dependent_patched SELECT number FROM numbers(3);
ALTER TABLE t_lwu_dependent_patched ADD COLUMN a UInt32 DEFAULT 0;
ALTER TABLE t_lwu_dependent_patched ADD COLUMN z UInt32 DEFAULT a + 100;
ALTER TABLE t_lwu_dependent_patched ADD COLUMN y UInt32 DEFAULT z + 1;

UPDATE t_lwu_dependent_patched SET z = 999 WHERE id = 0;
UPDATE t_lwu_dependent_patched SET y = 777 WHERE id = 1;
UPDATE t_lwu_dependent_patched SET a = 5 WHERE 1;

SELECT id, a, z, y FROM t_lwu_dependent_patched ORDER BY id SETTINGS max_threads = 1, max_block_size = 100;
SELECT id, a, z, y FROM t_lwu_dependent_patched ORDER BY id SETTINGS max_threads = 1, max_block_size = 1;

DROP TABLE t_lwu_dependent_patched;

-- The same, with the patches applied at the boundary of the reader step of a pending mutation.

SELECT 'pending mutation';

CREATE TABLE t_lwu_dependent_patched (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, index_granularity = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_dependent_patched SELECT number FROM numbers(3);
ALTER TABLE t_lwu_dependent_patched ADD COLUMN a UInt32 DEFAULT 0, ADD COLUMN w UInt32 DEFAULT 7;
ALTER TABLE t_lwu_dependent_patched ADD COLUMN z UInt32 DEFAULT a + 100;
ALTER TABLE t_lwu_dependent_patched ADD COLUMN y UInt32 DEFAULT z + 1;

SYSTEM STOP MERGES t_lwu_dependent_patched;

ALTER TABLE t_lwu_dependent_patched UPDATE w = 8 WHERE 1 SETTINGS mutations_sync = 0;
UPDATE t_lwu_dependent_patched SET z = 999 WHERE id = 0;
UPDATE t_lwu_dependent_patched SET y = 777 WHERE id = 1;
UPDATE t_lwu_dependent_patched SET a = 5 WHERE 1;

SELECT id, a, z, y, w FROM t_lwu_dependent_patched ORDER BY id SETTINGS max_threads = 1, max_block_size = 100, apply_mutations_on_fly = 1;
SELECT id, a, z, y, w FROM t_lwu_dependent_patched ORDER BY id SETTINGS max_threads = 1, max_block_size = 1, apply_mutations_on_fly = 1;

DROP TABLE t_lwu_dependent_patched;
