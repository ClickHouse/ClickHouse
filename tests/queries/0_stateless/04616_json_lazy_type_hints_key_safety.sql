-- Tags: no-fasttest

-- Safety checks for lazy JSON type-hint ALTERs (allow_experimental_json_lazy_type_hints).
-- A lazy metadata-only type-hint change skips the mutation branch, so it must be refused when it
-- changes the on-disk serialization of a value persisted positionally in the primary/partition
-- key or a secondary index (those are read back with the current type without per-part CAST).
-- The check is on the on-disk types of the subcolumns the key/index expression reads, not on the
-- expression result type: a type-sensitive expression such as reinterpretAsString(j.a) keeps the
-- same result type while its persisted bytes change. Changes on columns not feeding such a
-- structure, or that leave the on-disk type of every fed subcolumn unchanged, stay metadata-only.

SET enable_json_type = 1;
SET allow_experimental_json_lazy_type_hints = 1;
SET allow_suspicious_types_in_order_by = 1;

DROP TABLE IF EXISTS t_json_key_safety;

-- ============================================================
-- REJECT: typed-path subcolumn in ORDER BY, its type changes
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY (j.a, id);
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
SELECT 'ORDER BY subcolumn, type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: typed-path subcolumn in PARTITION BY, its type changes
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree PARTITION BY j.a ORDER BY id;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number % 3)) FROM numbers(9);
SELECT 'PARTITION BY subcolumn, type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: explicit skip index on the typed-path subcolumn, its type changes
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32), INDEX idx j.a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(8);
SELECT 'skip index on subcolumn, type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- ALLOW: add an unrelated typed path while the key is on an unchanged path
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY (j.a, id)
SETTINGS index_granularity = 4;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(64);
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int32, b String);
SELECT 'add unrelated path, key unchanged -> metadata-only mutations:',
    count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_json_key_safety';
DETACH TABLE t_json_key_safety;
ATTACH TABLE t_json_key_safety;
SELECT 'add unrelated path, count j.a >= 40 after reload (expect 24):',
    count() FROM t_json_key_safety WHERE j.a >= 40 SETTINGS force_primary_key = 1;
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT (conservative): key wraps the subcolumn in a value-preserving function.
-- toString(j.a) yields the same string for the affected values, but losslessness
-- of the persisted key value cannot be proven statically, so once j.a changes
-- type the ALTER is rejected.
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY (toString(j.a), id);
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
SELECT 'toString(j.a) key, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: key wraps the subcolumn in a type-sensitive function whose result type
-- is stable (String) but whose persisted bytes change with the subcolumn type,
-- e.g. reinterpretAsString(j.a) writes 4 bytes for Int32 and 8 for Int64 (and a
-- different string for negatives). A result-type-only check wrongly allowed this
-- and corrupted primary.idx.
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY (reinterpretAsString(j.a), id);
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
SELECT 'reinterpretAsString(j.a) key, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT (conservative): skip index wraps the subcolumn in toString(j.a).
-- ============================================================
CREATE TABLE t_json_key_safety
(id UInt32, j JSON(a Int32), INDEX idx toString(j.a) TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
SELECT 'toString(j.a) skip index, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- REJECT: skip index wraps the subcolumn in a type-sensitive reinterpretAsString(j.a).
-- ============================================================
CREATE TABLE t_json_key_safety
(id UInt32, j JSON(a Int32), INDEX idx reinterpretAsString(j.a) TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
SELECT 'reinterpretAsString(j.a) skip index, subcolumn type change -> reject:';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;

-- ============================================================
-- ALLOW: projection referencing the whole JSON column (subcolumns cannot be in a
-- projection and whole JSON cannot be a projection key, so it is safe)
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY id
SETTINGS deduplicate_merge_projection_mode = 'drop';
ALTER TABLE t_json_key_safety ADD PROJECTION p (SELECT id, j ORDER BY id);
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(16);
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64);
SELECT 'projection on whole column -> metadata-only mutations:',
    count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_json_key_safety';
DETACH TABLE t_json_key_safety;
ATTACH TABLE t_json_key_safety;
SELECT 'projection read after reload:', id, j.a FROM t_json_key_safety
ORDER BY id LIMIT 3 SETTINGS force_optimize_projection = 1;
DROP TABLE t_json_key_safety;

-- ============================================================
-- ALLOW: total type change on a plain (non-key) column reads back correctly
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64);
SELECT 'plain column total change -> metadata-only mutations:',
    count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_json_key_safety';
DETACH TABLE t_json_key_safety;
ATTACH TABLE t_json_key_safety;
SELECT 'plain column total change, read after reload:', id, j.a FROM t_json_key_safety ORDER BY id;
DROP TABLE t_json_key_safety;

-- ============================================================
-- ALLOW at ALTER time, FAIL on read: lossy type change on a plain column is not
-- validated at ALTER time (losslessness cannot be proven statically); the CAST
-- fails when the data is actually read.
-- ============================================================
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_key_safety VALUES (1, '{"a": "hello"}');
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int32);
SELECT 'lossy plain change -> metadata-only mutations:',
    count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_json_key_safety';
SELECT j.a FROM t_json_key_safety; -- { serverError CANNOT_PARSE_TEXT }
DROP TABLE t_json_key_safety;

-- ============================================================
-- Setting off: the same change on a key subcolumn goes through a full mutation
-- (existing non-lazy behavior), which forbids altering a key subcolumn.
-- ============================================================
SET allow_experimental_json_lazy_type_hints = 0;
CREATE TABLE t_json_key_safety (id UInt32, j JSON(a Int32)) ENGINE = MergeTree ORDER BY (j.a, id);
INSERT INTO t_json_key_safety SELECT number, toJSONString(map('a', number)) FROM numbers(4);
SELECT 'setting off, key subcolumn change -> reject (mutation path):';
ALTER TABLE t_json_key_safety MODIFY COLUMN j JSON(a Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key_safety;
