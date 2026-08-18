-- ALTER MODIFY COLUMN of a sort-key column may wrap or unwrap LowCardinality (the only
-- class-changing conversion allowed for key columns). Until the rewriting mutation
-- materializes, v2 patch parts and main parts hold sort-key columns of different classes:
-- patches written before the ALTER keep the old type, patches written after it carry the
-- new type, while main parts still store the old one.

SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;
SET mutations_sync = 0;
SET alter_sync = 0;

DROP TABLE IF EXISTS t_lwu_key_type SYNC;

-- LowCardinality(String) -> String.
CREATE TABLE t_lwu_key_type (s LowCardinality(String), id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY (s, id)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
         patch_parts_version = 'v2', apply_patches_on_merge = 1;

INSERT INTO t_lwu_key_type SELECT concat('key_', toString(number % 100)), number, 0 FROM numbers(10000);

-- Keep the rewriting mutation of the ALTER pending.
SYSTEM STOP MERGES t_lwu_key_type;

-- Patch written while the key column is LowCardinality(String).
UPDATE t_lwu_key_type SET v = 1 WHERE id % 10 = 0;

ALTER TABLE t_lwu_key_type MODIFY COLUMN s String;

-- Patch with the old key type over main parts with the old key type.
SELECT sum(v), countIf(v = 1) FROM t_lwu_key_type;

-- Patch written after the ALTER carries the key column as String.
UPDATE t_lwu_key_type SET v = 2 WHERE id % 20 = 0;

-- Patches of both key types over main parts with the old key type.
SELECT sum(v), countIf(v = 2), countIf(v = 1) FROM t_lwu_key_type;

SYSTEM START MERGES t_lwu_key_type;

-- Barrier: mutations run in order, so waiting for this one waits for the ALTER as well.
ALTER TABLE t_lwu_key_type UPDATE v = v WHERE 0 SETTINGS mutations_sync = 2;

-- Patch with the old key type over main parts rewritten to the new key type.
SELECT sum(v), countIf(v = 2), countIf(v = 1) FROM t_lwu_key_type;

-- Materialize the patches (the barrier mutation or this merge applies them).
OPTIMIZE TABLE t_lwu_key_type FINAL;
SELECT sum(v), countIf(v = 2), countIf(v = 1) FROM t_lwu_key_type SETTINGS apply_patch_parts = 0;

DROP TABLE t_lwu_key_type SYNC;

-- The opposite direction: String -> LowCardinality(String).
CREATE TABLE t_lwu_key_type (s String, id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY (s, id)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

INSERT INTO t_lwu_key_type SELECT concat('key_', toString(number % 100)), number, 0 FROM numbers(10000);

SYSTEM STOP MERGES t_lwu_key_type;

UPDATE t_lwu_key_type SET v = 1 WHERE id % 10 = 0;

ALTER TABLE t_lwu_key_type MODIFY COLUMN s LowCardinality(String);

SELECT sum(v), countIf(v = 1) FROM t_lwu_key_type;

UPDATE t_lwu_key_type SET v = 2 WHERE id % 20 = 0;

SELECT sum(v), countIf(v = 2), countIf(v = 1) FROM t_lwu_key_type;

SYSTEM START MERGES t_lwu_key_type;

ALTER TABLE t_lwu_key_type UPDATE v = v WHERE 0 SETTINGS mutations_sync = 2;

SELECT sum(v), countIf(v = 2), countIf(v = 1) FROM t_lwu_key_type;

DROP TABLE t_lwu_key_type SYNC;

-- A column used inside a sort-key expression cannot be altered to another type at all,
-- so the classes of sort-key columns can diverge only for plain key columns.
CREATE TABLE t_lwu_key_type (s LowCardinality(String), id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY (lower(s), id)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

ALTER TABLE t_lwu_key_type MODIFY COLUMN s String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_lwu_key_type SYNC;
