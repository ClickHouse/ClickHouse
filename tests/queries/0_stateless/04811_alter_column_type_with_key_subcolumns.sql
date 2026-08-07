-- Allow ALTER MODIFY COLUMN of a column whose subcolumns feed the primary/partition key, as long
-- as the key subcolumns keep an on-disk-compatible type (isSafeForKeyConversion). Other subcolumns
-- of the column may change freely. A subcolumn used inside a key expression is forbidden, same as a
-- plain column used in a key expression. The check is type-agnostic (JSON, Tuple, ...).

SET allow_suspicious_types_in_order_by = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_alter_key_sub;

-- ============================================================
-- JSON: subcolumn data.a in ORDER BY, kept unchanged.
-- Also check the mutation materializes and the on-disk primary index is correct after a reload.
-- ============================================================
CREATE TABLE t_alter_key_sub (id UInt32, data JSON(a Int64)) ENGINE = MergeTree ORDER BY (data.a, id) SETTINGS index_granularity = 2;
INSERT INTO t_alter_key_sub SELECT number, toJSONString(map('a', number)) FROM numbers(6);

SELECT 'add unrelated typed path, key subcolumn unchanged -> allowed:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(a Int64, b String);
SELECT 'pending mutations:', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_alter_key_sub' AND NOT is_done;
DETACH TABLE t_alter_key_sub;
ATTACH TABLE t_alter_key_sub;
SELECT 'key lookup after reload (data.a = 4):';
SELECT id, data.a FROM t_alter_key_sub WHERE data.a = 4;

SELECT 'adjust JSON param, key subcolumn unchanged -> allowed:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(max_dynamic_paths = 0, a Int64, b String);
SELECT 'SKIP unrelated path, key subcolumn unchanged -> allowed:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(a Int64, SKIP b);
SELECT id, data.a FROM t_alter_key_sub ORDER BY id;

SELECT 'change key subcolumn type unsafely -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(a Int32); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_alter_key_sub;

-- ============================================================
-- JSON: subcolumn data.a used inside a key expression. Allowed while data.a is unchanged (the key
-- expression stays identical), rejected when data.a changes unsafely.
-- ============================================================
CREATE TABLE t_alter_key_sub (id UInt32, data JSON(a Int64)) ENGINE = MergeTree ORDER BY (toString(data.a), id);
INSERT INTO t_alter_key_sub SELECT number, toJSONString(map('a', number)) FROM numbers(4);
SELECT 'subcolumn in key expression, add unrelated path (data.a unchanged) -> allowed:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(a Int64, b String);
SELECT id, data.a FROM t_alter_key_sub ORDER BY id;
SELECT 'subcolumn in key expression, retype data.a -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(a Int32); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_alter_key_sub;

-- ============================================================
-- JSON: subcolumn data.a in PARTITION BY, kept unchanged
-- ============================================================
CREATE TABLE t_alter_key_sub (id UInt32, data JSON(a Int64)) ENGINE = MergeTree PARTITION BY data.a ORDER BY id;
INSERT INTO t_alter_key_sub SELECT number, toJSONString(map('a', number % 3)) FROM numbers(9);

SELECT 'add unrelated typed path, partition subcolumn unchanged -> allowed:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(a Int64, b String);
SELECT count() FROM t_alter_key_sub;

SELECT 'change partition subcolumn type unsafely -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(a Int32); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_alter_key_sub;

-- ============================================================
-- Tuple (type-agnostic): subcolumn t.a in ORDER BY
-- ============================================================
CREATE TABLE t_alter_key_sub (id UInt32, t Tuple(a Int32, b Int32)) ENGINE = MergeTree ORDER BY (t.a, id);
INSERT INTO t_alter_key_sub SELECT number, (number, number) FROM numbers(4);

SELECT 'change non-key subcolumn only -> allowed:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN t Tuple(a Int32, b Int64);
SELECT id, t.a, t.b FROM t_alter_key_sub ORDER BY id;

SELECT 'change key subcolumn unsafely -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN t Tuple(a Int64, b Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_alter_key_sub;

-- ============================================================
-- Tuple: key subcolumn changed in an on-disk-compatible way (enum widening) -> allowed
-- ============================================================
CREATE TABLE t_alter_key_sub (id UInt32, t Tuple(a Enum8('x' = 1, 'y' = 2), b Int32)) ENGINE = MergeTree ORDER BY (t.a, id);
INSERT INTO t_alter_key_sub VALUES (1, ('x', 10)), (2, ('y', 20));
SELECT 'widen key subcolumn enum (safe) -> allowed:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN t Tuple(a Enum8('x' = 1, 'y' = 2, 'z' = 3), b Int32);
SELECT id, t.a, t.b FROM t_alter_key_sub ORDER BY t.a, id;
DROP TABLE t_alter_key_sub;
