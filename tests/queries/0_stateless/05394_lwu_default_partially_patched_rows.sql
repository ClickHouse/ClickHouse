-- A lightweight `UPDATE` overwrites only some rows of a `DEFAULT` column the part does not store. The other rows
-- still hold the value of its `DEFAULT` and must follow a later lightweight `UPDATE` of the column it reads,
-- regardless of which rows are read together in one block.

DROP TABLE IF EXISTS t_lwu_partial_default;

SELECT 'v1';

CREATE TABLE t_lwu_partial_default (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, index_granularity = 1, patch_parts_version = 'v1';

INSERT INTO t_lwu_partial_default SELECT number FROM numbers(2);
ALTER TABLE t_lwu_partial_default ADD COLUMN a UInt32 DEFAULT 0;
ALTER TABLE t_lwu_partial_default ADD COLUMN z UInt32 DEFAULT a + 100;
ALTER TABLE t_lwu_partial_default ADD COLUMN y UInt32 DEFAULT z + 1;

UPDATE t_lwu_partial_default SET z = 999 WHERE id = 0;
UPDATE t_lwu_partial_default SET a = 5 WHERE 1;

SELECT id, a, z, y FROM t_lwu_partial_default ORDER BY id SETTINGS max_threads = 1, max_block_size = 100;
SELECT id, a, z, y FROM t_lwu_partial_default ORDER BY id SETTINGS max_threads = 1, max_block_size = 1;
SELECT id, z FROM t_lwu_partial_default ORDER BY id SETTINGS max_threads = 1, max_block_size = 100;
SELECT id, y FROM t_lwu_partial_default WHERE id = 1;

DROP TABLE t_lwu_partial_default;

SELECT 'v2';

CREATE TABLE t_lwu_partial_default (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, index_granularity = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_partial_default SELECT number FROM numbers(2);
ALTER TABLE t_lwu_partial_default ADD COLUMN a UInt32 DEFAULT 0;
ALTER TABLE t_lwu_partial_default ADD COLUMN z UInt32 DEFAULT a + 100;
ALTER TABLE t_lwu_partial_default ADD COLUMN y UInt32 DEFAULT z + 1;

UPDATE t_lwu_partial_default SET z = 999 WHERE id = 0;
UPDATE t_lwu_partial_default SET a = 5 WHERE 1;

SELECT id, a, z, y FROM t_lwu_partial_default ORDER BY id SETTINGS max_threads = 1, max_block_size = 100;
SELECT id, a, z, y FROM t_lwu_partial_default ORDER BY id SETTINGS max_threads = 1, max_block_size = 1;
SELECT id, z FROM t_lwu_partial_default ORDER BY id SETTINGS max_threads = 1, max_block_size = 100;
SELECT id, y FROM t_lwu_partial_default WHERE id = 1;

DROP TABLE t_lwu_partial_default;
