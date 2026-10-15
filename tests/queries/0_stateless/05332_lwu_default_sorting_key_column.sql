DROP TABLE IF EXISTS t_lwu_default_key;
DROP TABLE IF EXISTS t_mutation_default_key;

-- The sort-key column `k` is not stored in the part, and its `DEFAULT` reads `a`. With `patch_parts_version = 'v2'`
-- the patch is applied by `MergeOnKey`, which fills `k` with its default before any patch is applied.
-- After a lightweight `UPDATE` of `a`, `k` must be evaluated from the patched value, as after `ALTER TABLE ... UPDATE`.

CREATE TABLE t_lwu_default_key (id UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v2';

CREATE TABLE t_mutation_default_key (id UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_lwu_default_key SELECT number FROM numbers(6);
INSERT INTO t_mutation_default_key SELECT number FROM numbers(6);

ALTER TABLE t_lwu_default_key ADD COLUMN a UInt32 DEFAULT 0, ADD COLUMN k UInt32, MODIFY ORDER BY (id, k);
ALTER TABLE t_lwu_default_key MODIFY COLUMN k UInt32 DEFAULT a + 100;

ALTER TABLE t_mutation_default_key ADD COLUMN a UInt32 DEFAULT 0, ADD COLUMN k UInt32, MODIFY ORDER BY (id, k);
ALTER TABLE t_mutation_default_key MODIFY COLUMN k UInt32 DEFAULT a + 100;

UPDATE t_lwu_default_key SET a = id + 5 WHERE id % 2 = 0;
ALTER TABLE t_mutation_default_key UPDATE a = id + 5 WHERE id % 2 = 0 SETTINGS mutations_sync = 2;

SELECT 'lightweight';
SELECT id, a, k FROM t_lwu_default_key ORDER BY id;
SELECT id, k FROM t_lwu_default_key ORDER BY id;
SELECT sum(k) FROM t_lwu_default_key;

SELECT 'heavy';
SELECT id, a, k FROM t_mutation_default_key ORDER BY id;
SELECT id, k FROM t_mutation_default_key ORDER BY id;
SELECT sum(k) FROM t_mutation_default_key;

DROP TABLE t_lwu_default_key;
DROP TABLE t_mutation_default_key;

-- The same, but `a` is stored in the part, so its patch is applied before the defaults are evaluated,
-- while `k` was already filled with its default before any patch was applied.

DROP TABLE IF EXISTS t_lwu_default_key_stored;
DROP TABLE IF EXISTS t_mutation_default_key_stored;

CREATE TABLE t_lwu_default_key_stored (id UInt32, a UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0, patch_parts_version = 'v2';

CREATE TABLE t_mutation_default_key_stored (id UInt32, a UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_lwu_default_key_stored SELECT number, 0 FROM numbers(6);
INSERT INTO t_mutation_default_key_stored SELECT number, 0 FROM numbers(6);

ALTER TABLE t_lwu_default_key_stored ADD COLUMN k UInt32, MODIFY ORDER BY (id, k);
ALTER TABLE t_lwu_default_key_stored MODIFY COLUMN k UInt32 DEFAULT a + 100;

ALTER TABLE t_mutation_default_key_stored ADD COLUMN k UInt32, MODIFY ORDER BY (id, k);
ALTER TABLE t_mutation_default_key_stored MODIFY COLUMN k UInt32 DEFAULT a + 100;

UPDATE t_lwu_default_key_stored SET a = id + 5 WHERE id % 2 = 0;
ALTER TABLE t_mutation_default_key_stored UPDATE a = id + 5 WHERE id % 2 = 0 SETTINGS mutations_sync = 2;

SELECT 'lightweight, stored input';
SELECT id, a, k FROM t_lwu_default_key_stored ORDER BY id;
SELECT id, k FROM t_lwu_default_key_stored ORDER BY id;
SELECT sum(k) FROM t_lwu_default_key_stored;

SELECT 'heavy, stored input';
SELECT id, a, k FROM t_mutation_default_key_stored ORDER BY id;
SELECT id, k FROM t_mutation_default_key_stored ORDER BY id;
SELECT sum(k) FROM t_mutation_default_key_stored;

DROP TABLE t_lwu_default_key_stored;
DROP TABLE t_mutation_default_key_stored;
