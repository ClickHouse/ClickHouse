DROP TABLE IF EXISTS t_lwu_default_key;
DROP TABLE IF EXISTS t_lwu_default_key_stored;

-- The sort-key column `k` is not stored in the part, and its `DEFAULT` reads `a`. With `patch_parts_version = 'v2'`
-- the patch is applied by `MergeOnKey`, which fills `k` with its default before any patch is applied.
-- The rows of the part are sorted by that value of `k`, so `k` keeps it after a lightweight `UPDATE` of `a`:
-- computing it again would change the key without reordering the rows. The rows with equal `id` check that
-- ordered reads and the part written by a merge keep the order of the key, and `PREWHERE` checks that a key
-- read by an earlier step does not differ from the one read by the last step.

CREATE TABLE t_lwu_default_key (id UInt32, x UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v2';

INSERT INTO t_lwu_default_key VALUES (0, 0), (0, 1), (1, 0), (1, 1);
SYSTEM STOP MERGES t_lwu_default_key;

ALTER TABLE t_lwu_default_key ADD COLUMN a UInt32 DEFAULT 0, ADD COLUMN k UInt32, MODIFY ORDER BY (id, k);
ALTER TABLE t_lwu_default_key MODIFY COLUMN k UInt32 DEFAULT a + 100;

UPDATE t_lwu_default_key SET a = 10 WHERE x = 0;

SELECT 'missing input';
SELECT id, x, a, k FROM t_lwu_default_key ORDER BY id, x;
SELECT id, x, k FROM t_lwu_default_key ORDER BY id, x;
SELECT id, x, a, k FROM t_lwu_default_key ORDER BY id, k, x SETTINGS optimize_read_in_order = 1;
SELECT id, x, a, k FROM t_lwu_default_key PREWHERE id = 0 ORDER BY x;

ALTER TABLE t_lwu_default_key MODIFY SETTING apply_patches_on_merge = 1;
SYSTEM START MERGES t_lwu_default_key;
OPTIMIZE TABLE t_lwu_default_key FINAL;

SELECT 'missing input, merged';
SELECT id, x, a, k FROM t_lwu_default_key ORDER BY id, k, x SETTINGS optimize_read_in_order = 1, apply_patch_parts = 0;

-- The same, but `a` is stored in the part, so its patch is applied before the other defaults are evaluated.

CREATE TABLE t_lwu_default_key_stored (id UInt32, x UInt32, a UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v2';

INSERT INTO t_lwu_default_key_stored VALUES (0, 0, 0), (0, 1, 0), (1, 0, 0), (1, 1, 0);
SYSTEM STOP MERGES t_lwu_default_key_stored;

ALTER TABLE t_lwu_default_key_stored ADD COLUMN k UInt32, MODIFY ORDER BY (id, k);
ALTER TABLE t_lwu_default_key_stored MODIFY COLUMN k UInt32 DEFAULT a + 100;

UPDATE t_lwu_default_key_stored SET a = 10 WHERE x = 0;

SELECT 'stored input';
SELECT id, x, a, k FROM t_lwu_default_key_stored ORDER BY id, x;
SELECT id, x, k FROM t_lwu_default_key_stored ORDER BY id, x;
SELECT id, x, a, k FROM t_lwu_default_key_stored ORDER BY id, k, x SETTINGS optimize_read_in_order = 1;
SELECT id, x, a, k FROM t_lwu_default_key_stored PREWHERE id = 0 ORDER BY x;

ALTER TABLE t_lwu_default_key_stored MODIFY SETTING apply_patches_on_merge = 1;
SYSTEM START MERGES t_lwu_default_key_stored;
OPTIMIZE TABLE t_lwu_default_key_stored FINAL;

SELECT 'stored input, merged';
SELECT id, x, a, k FROM t_lwu_default_key_stored ORDER BY id, k, x SETTINGS optimize_read_in_order = 1, apply_patch_parts = 0;

DROP TABLE t_lwu_default_key;
DROP TABLE t_lwu_default_key_stored;
