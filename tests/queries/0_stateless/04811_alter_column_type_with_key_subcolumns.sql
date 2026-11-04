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

-- Removing a key subcolumn is rejected: the sorting key can no longer resolve it.
SELECT 'remove key subcolumn -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN t Tuple(b Int32); -- { serverError UNKNOWN_IDENTIFIER }

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

-- ============================================================
-- A key subcolumn below a Nullable level is Nullable either way, so its type can stay the same while
-- the ALTER turns its NULLs into defaults. Each level on the way to the key subcolumn is checked.
-- ============================================================
CREATE TABLE t_alter_key_sub (id UInt32, data JSON(a Nullable(Int64))) ENGINE = MergeTree ORDER BY (data.a, id) SETTINGS index_granularity = 2, allow_nullable_key = 1;
INSERT INTO t_alter_key_sub SELECT number, toJSONString(map('a', number)) FROM numbers(6);
INSERT INTO t_alter_key_sub SELECT number + 6, '{}' FROM numbers(6);
OPTIMIZE TABLE t_alter_key_sub FINAL;
SELECT 'add Nullable level, key subcolumn loses its Nullable -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data Nullable(JSON(a Int64)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values (NULL, = 0, total):', countIf(data.a IS NULL), countIf(data.a = 0), count() FROM t_alter_key_sub;
DROP TABLE t_alter_key_sub;

-- Allowed changes first: no key lookup may run after an ALTER that left the part unsorted.
CREATE TABLE t_alter_key_sub (id UInt32, data JSON(a Nullable(Int64))) ENGINE = MergeTree ORDER BY (data.a, id) SETTINGS index_granularity = 2, allow_nullable_key = 1;
INSERT INTO t_alter_key_sub SELECT number, toJSONString(map('a', number)) FROM numbers(6);
INSERT INTO t_alter_key_sub SELECT number + 6, '{}' FROM numbers(6);
OPTIMIZE TABLE t_alter_key_sub FINAL;
SELECT 'add Nullable level, key subcolumn keeps its Nullable -> allowed:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data Nullable(JSON(a Nullable(Int64)));
SELECT 'add unrelated typed path under a kept Nullable level -> allowed:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data Nullable(JSON(a Nullable(Int64), b String));
SELECT 'same, as a lazy type hint change -> allowed:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data Nullable(JSON(a Nullable(Int64), b String, c String)) SETTINGS enable_json_lazy_type_hints = 1;
SELECT 'type:', type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_key_sub' AND name = 'data';
SELECT 'key lookup (data.a = 0):', count() FROM t_alter_key_sub WHERE data.a = 0;
SELECT 'key subcolumn loses its Nullable under a kept Nullable level -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data Nullable(JSON(a Int64, b String, c String)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'same, as a lazy type hint change -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data Nullable(JSON(a Int64, b String, c String)) SETTINGS enable_json_lazy_type_hints = 1; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'remove the Nullable level with a DEFAULT -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(a Nullable(Int64), b String, c String) DEFAULT '{"a" : 100}'; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values (NULL, = 0, total):', countIf(data.a IS NULL), countIf(data.a = 0), count() FROM t_alter_key_sub;
DROP TABLE t_alter_key_sub;

-- Partition key.
CREATE TABLE t_alter_key_sub (id UInt32, data JSON(a Nullable(Int64))) ENGINE = MergeTree PARTITION BY data.a ORDER BY id SETTINGS index_granularity = 2, allow_nullable_key = 1;
INSERT INTO t_alter_key_sub SELECT number, if(number < 6, toJSONString(map('a', number % 3)), '{}') FROM numbers(12);
SELECT 'partition key, add Nullable level, key subcolumn loses its Nullable -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data Nullable(JSON(a Int64)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_alter_key_sub;

-- Nullable level below a Tuple element.
CREATE TABLE t_alter_key_sub (id UInt32, t Tuple(j JSON(a Nullable(Int64)), b Int32)) ENGINE = MergeTree ORDER BY (t.j.a, id) SETTINGS index_granularity = 2, allow_nullable_key = 1;
INSERT INTO t_alter_key_sub SELECT number, (if(number < 6, toJSONString(map('a', number)), '{}'), 1) FROM numbers(12);
OPTIMIZE TABLE t_alter_key_sub FINAL;
SELECT 'Tuple element gets a Nullable level, key subcolumn loses its Nullable -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN t Tuple(j Nullable(JSON(a Int64)), b Int32); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'change non-key Tuple element -> allowed:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN t Tuple(j JSON(a Nullable(Int64)), b Int64);
SELECT 'type:', type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_key_sub' AND name = 't';
SELECT 'key subcolumn values (NULL, = 0, total):', countIf(t.j.a IS NULL), countIf(t.j.a = 0), count() FROM t_alter_key_sub;
DROP TABLE t_alter_key_sub;

-- Nullable level below an Array.
CREATE TABLE t_alter_key_sub (id UInt32, arr Array(JSON(a Nullable(Int64)))) ENGINE = MergeTree ORDER BY (arr.a, id) SETTINGS index_granularity = 2, allow_nullable_key = 1;
INSERT INTO t_alter_key_sub SELECT number, [if(number < 6, toJSONString(map('a', number)), '{}')] FROM numbers(12);
OPTIMIZE TABLE t_alter_key_sub FINAL;
SELECT 'Array element gets a Nullable level, key subcolumn loses its Nullable -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN arr Array(Nullable(JSON(a Int64))); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_alter_key_sub;

-- Skip index on the subcolumn, lazy type hint change.
CREATE TABLE t_alter_key_sub (id UInt32, data Nullable(JSON(a Nullable(Int64))), INDEX i data.a TYPE minmax) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO t_alter_key_sub SELECT number, if(number < 6, toJSONString(map('a', number)), '{}') FROM numbers(12);
SELECT 'skip index, key subcolumn loses its Nullable under a kept Nullable level, lazy -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data Nullable(JSON(a Int64)) SETTINGS enable_json_lazy_type_hints = 1; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_alter_key_sub;

-- Subcolumn names are flat: the element `j.a` and the path `a` of the element `j` both answer `c.j.a`.
CREATE TABLE t_alter_key_sub (id UInt32, c Tuple(`j.a` Int64, j JSON(a Int64))) ENGINE = MergeTree ORDER BY (c.`j.a`, id) SETTINGS index_granularity = 2;
INSERT INTO t_alter_key_sub VALUES (0, (0, '{"a" : 2}')), (1, (1, '{"a" : 1}')), (2, (2, '{"a" : 0}'));
SELECT 'drop the key element, its name resolves to a sibling path -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN c Tuple(j JSON(a Int64)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_alter_key_sub;

CREATE TABLE t_alter_key_sub (id UInt32, c Tuple(`j.a` Int64, j JSON(a Nullable(Int64)))) ENGINE = MergeTree ORDER BY (c.`j.a`, id) SETTINGS index_granularity = 2;
INSERT INTO t_alter_key_sub VALUES (0, (0, '{"a" : 2}')), (1, (1, '{}')), (2, (2, '{"a" : 0}'));
SELECT 'change the sibling path, the key element is kept -> allowed:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN c Tuple(`j.a` Int64, j JSON(a Int64));
SELECT 'type:', type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_key_sub' AND name = 'c';
SELECT 'key lookup (c.`j.a` = 1):', count() FROM t_alter_key_sub WHERE c.`j.a` = 1;
DROP TABLE t_alter_key_sub;

-- A key on a dynamic path read with a type hint.
CREATE TABLE t_alter_key_sub (id UInt32, data JSON) ENGINE = MergeTree ORDER BY (data.b.:Int64, id) SETTINGS index_granularity = 2, allow_nullable_key = 1;
INSERT INTO t_alter_key_sub SELECT number, toJSONString(map('b', number)) FROM numbers(6);
SELECT 'dynamic path with a type hint, other paths change -> allowed:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(c String);
ALTER TABLE t_alter_key_sub MODIFY COLUMN data Nullable(JSON(c String));
SELECT 'type:', type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_key_sub' AND name = 'data';
SELECT 'key lookup (data.b.:Int64 = 4):', count() FROM t_alter_key_sub WHERE data.b.:Int64 = 4;
DROP TABLE t_alter_key_sub;

-- A Map value read by its key: `key_1` is parsed as the key type.
CREATE TABLE t_alter_key_sub (id UInt32, m Map(String, Int64)) ENGINE = MergeTree ORDER BY (m.key_1, id) SETTINGS index_granularity = 1;
INSERT INTO t_alter_key_sub VALUES (0, map('1', 0)), (1, map('01', 3, '1', 1)), (2, map('1', 2));
SELECT 'Map key type change, the key subcolumn reads another entry -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN m Map(Int64, Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(k) FROM (SELECT m.key_1 AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

CREATE TABLE t_alter_key_sub (id UInt32, m Map(String, Int64)) ENGINE = MergeTree ORDER BY (m.values, id) SETTINGS index_granularity = 1;
INSERT INTO t_alter_key_sub VALUES (0, map('1', 0)), (1, map('2', 1)), (2, map('3', 2));
SELECT 'Map key type change, the key subcolumn does not depend on it -> allowed:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN m Map(Int64, Int64);
SELECT 'type:', type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_key_sub' AND name = 'm';
SELECT 'key lookup (m.values = [1]):', count() FROM t_alter_key_sub WHERE m.values = [1];
DROP TABLE t_alter_key_sub;

-- A key on a Variant subcolumn.
CREATE TABLE t_alter_key_sub (id UInt32, v Variant(Int64, String)) ENGINE = MergeTree ORDER BY (v.Int64, id) SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_alter_key_sub VALUES (0, CAST(toInt64(0), 'Variant(Int64, String)')), (1, CAST('x', 'Variant(Int64, String)')), (2, CAST(toInt64(2), 'Variant(Int64, String)'));
SELECT 'Variant to Dynamic, the key subcolumn keeps its values -> allowed:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN v Dynamic;
SELECT 'type:', type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_key_sub' AND name = 'v';
SELECT 'key lookup (v.Int64 = 2):', count() FROM t_alter_key_sub WHERE v.Int64 = 2;
DROP TABLE t_alter_key_sub;
