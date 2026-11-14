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

-- A key on a dynamic path read with a type hint. A JSON -> JSON conversion infers the types of its values again:
-- 2.0 is written as 2 and comes back as Int64.
CREATE TABLE t_alter_key_sub (id UInt32, data JSON) ENGINE = MergeTree ORDER BY (data.b.:Float64, id) SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_alter_key_sub VALUES (0, '{"b" : 0.5}'), (1, '{"b" : 1.5}'), (2, '{"b" : 2.0}');
SELECT 'dynamic path with a type hint, other paths change -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(c String); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(k) FROM (SELECT data.b.:Float64 AS k FROM t_alter_key_sub ORDER BY id);
SELECT 'dynamic path with a type hint, only a Nullable level is added -> allowed:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data Nullable(JSON);
SELECT 'type:', type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_key_sub' AND name = 'data';
SELECT 'key lookup (data.b.:Float64 = 1.5):', count() FROM t_alter_key_sub WHERE data.b.:Float64 = 1.5;
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

-- A JSON -> JSON conversion parses the data again with the skip rules of the new type. A typed path cannot be a SKIP
-- path, but a SKIP REGEXP can match a prefix of it or a part of it.
CREATE TABLE t_alter_key_sub (id UInt32, data JSON(b.c Int64)) ENGINE = MergeTree ORDER BY (data.b.c, id) SETTINGS index_granularity = 1;
INSERT INTO t_alter_key_sub SELECT number, toJSONString(map('b', map('c', number))) FROM numbers(3);
SELECT 'JSON skip rule for a prefix of the key path or a part of it -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(b.c Int64, SKIP REGEXP '^b$'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(b.c Int64, SKIP REGEXP 'c'); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(k) FROM (SELECT data.b.c AS k FROM t_alter_key_sub ORDER BY id);
SELECT 'JSON skip rule for another path -> allowed:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(b.c Int64, SKIP c);
SELECT 'type:', type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_key_sub' AND name = 'data';
SELECT 'key lookup (data.b.c = 1):', count() FROM t_alter_key_sub WHERE data.b.c = 1;
DROP TABLE t_alter_key_sub;

-- A typed path takes the whole object below it: a dynamic key path below it loses its values when the typed path is
-- added and gets values when it is removed.
CREATE TABLE t_alter_key_sub (id UInt32, data JSON) ENGINE = MergeTree ORDER BY (data.b.c.:Int64, id) SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_alter_key_sub SELECT number, toJSONString(map('b', map('c', number))) FROM numbers(3);
SELECT 'typed path above a dynamic key path is added -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(b String); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(k) FROM (SELECT data.b.c.:Int64 AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

CREATE TABLE t_alter_key_sub (id UInt32, data JSON(b Map(String, Int64))) ENGINE = MergeTree ORDER BY (data.b.c.:Int64, id) SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_alter_key_sub SELECT number, toJSONString(map('b', map('c', number))) FROM numbers(3);
SELECT 'typed path above a dynamic key path is removed -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn NULLs:', countIf(data.b.c.:Int64 IS NULL) FROM t_alter_key_sub;
DROP TABLE t_alter_key_sub;

-- A combined subcolumn (`@`) in a key expression reads a Dynamic value.
CREATE TABLE t_alter_key_sub (id UInt32, data JSON) ENGINE = MergeTree ORDER BY (toString(data.@a), id) SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_alter_key_sub VALUES (0, '{"a" : 0}'), (1, '{"a" : 1}'), (2, '{"a" : 2}');
SELECT 'combined subcolumn in a key expression, JSON type change -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(SKIP a); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(toString(k)) FROM (SELECT data.@a AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

-- A typed path of type Dynamic: its values get their types inferred again.
CREATE TABLE t_alter_key_sub (id UInt32, data JSON(a Dynamic)) ENGINE = MergeTree ORDER BY (data.a.:Float64, id) SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_alter_key_sub VALUES (0, '{"a" : 0.5}'), (1, '{"a" : 1.5}'), (2, '{"a" : 2.0}');
SELECT 'typed Dynamic path, other paths change -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(a Dynamic, c String); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(k) FROM (SELECT data.a.:Float64 AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

-- A typed path takes the whole object below it, also when the key path below it is typed.
CREATE TABLE t_alter_key_sub (id UInt32, data JSON(b.c Int64)) ENGINE = MergeTree ORDER BY (data.b.c, id) SETTINGS index_granularity = 1;
INSERT INTO t_alter_key_sub SELECT number, toJSONString(map('b', map('c', number))) FROM numbers(3);
SELECT 'typed path above a typed key path is added -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(b String, b.c Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(k) FROM (SELECT data.b.c AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

-- A typed Variant path: the variant is chosen again, so the String '1' comes back as Int64 1. A text INSERT already
-- stores it as Int64, so the rows are read from RowBinary.
CREATE TABLE t_alter_key_sub (id UInt32, data JSON(a Variant(Int64, String))) ENGINE = MergeTree ORDER BY (data.a.String, id) SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_alter_key_sub SELECT * FROM format(RowBinary, 'id UInt32, data JSON(a Variant(Int64, String))', unhex('000000000101610000000000000000000100000001016101013102000000010161010178'));
SELECT 'typed Variant path, other paths change -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(a Variant(Int64, String), c String); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(k) FROM (SELECT data.a.String AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

-- A typed Float64 path: inf and nan are written as null, which comes back as 0.
CREATE TABLE t_alter_key_sub (id UInt32, data JSON(a Float64)) ENGINE = MergeTree ORDER BY (data.a, id) SETTINGS index_granularity = 1;
INSERT INTO t_alter_key_sub VALUES (0, '{"a" : "-inf"}'), (1, '{"a" : 1}'), (2, '{"a" : "inf"}');
SELECT 'typed Float64 path, other paths change -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(a Float64, c String); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(k) FROM (SELECT data.a AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

-- A typed Decimal path: a value with a fraction is written as a JSON number and parsed through Float64.
CREATE TABLE t_alter_key_sub (id UInt32, data JSON(a Decimal(38, 2))) ENGINE = MergeTree ORDER BY (data.a, id) SETTINGS index_granularity = 1;
INSERT INTO t_alter_key_sub VALUES (0, '{"a" : "12345678901234567.81"}'), (1, '{"a" : "12345678901234567.82"}'), (2, '{"a" : "12345678901234567.83"}');
SELECT 'typed Decimal path, other paths change -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(a Decimal(38, 2), c String); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(k) FROM (SELECT data.a AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

-- A typed Enum path is written as its names, which an integer type parses as numbers.
CREATE TABLE t_alter_key_sub (id UInt32, data JSON(a Enum8('2' = 0, '0' = 1, '1' = 2))) ENGINE = MergeTree ORDER BY (data.a, id) SETTINGS index_granularity = 1;
INSERT INTO t_alter_key_sub VALUES (0, '{"a" : "2"}'), (1, '{"a" : "0"}'), (2, '{"a" : "1"}');
SELECT 'typed Enum path to an integer type -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(a Int8); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(toInt8(k)) FROM (SELECT data.a AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

-- A typed DateTime path is written as local time: the hour repeated when the clocks go back reads back as one hour.
CREATE TABLE t_alter_key_sub (id UInt32, data JSON(a DateTime('Europe/Berlin'))) ENGINE = MergeTree ORDER BY (data.a, id) SETTINGS index_granularity = 1;
INSERT INTO t_alter_key_sub VALUES (0, '{"a" : 1729989000}'), (1, '{"a" : 1729990800}'), (2, '{"a" : 1729992600}');
SELECT 'typed DateTime path, other paths change -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(a DateTime('Europe/Berlin'), c String); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(toUnixTimestamp(k)) FROM (SELECT data.a AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

-- A typed Bool or Date32 path: a value out of range, read from a binary format, is clamped by the text round trip.
CREATE TABLE t_alter_key_sub (id UInt32, data JSON(a Bool)) ENGINE = MergeTree ORDER BY (data.a, id) SETTINGS index_granularity = 1;
INSERT INTO t_alter_key_sub SELECT * FROM format(RowBinary, 'id UInt32, data JSON(a Bool)', unhex('000000000101610001000000010161010200000001016102'));
SELECT 'typed Bool path, other paths change -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(a Bool, c String); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(reinterpretAsUInt8(k)) FROM (SELECT data.a AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

-- The storage type of a SimpleAggregateFunction keeps the Bool serialization.
CREATE TABLE t_alter_key_sub (id UInt32, data JSON(a SimpleAggregateFunction(any, Bool))) ENGINE = MergeTree PARTITION BY data.a ORDER BY id;
INSERT INTO t_alter_key_sub SELECT * FROM format(RowBinary, 'id UInt32, data JSON(a SimpleAggregateFunction(any, Bool))', unhex('000000000101610001000000010161010200000001016102'));
SELECT 'typed SimpleAggregateFunction(any, Bool) path in the partition key, other paths change -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(a SimpleAggregateFunction(any, Bool), c String); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(reinterpretAsUInt8(k)) FROM (SELECT data.a AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

CREATE TABLE t_alter_key_sub (id UInt32, data JSON(a Date32)) ENGINE = MergeTree ORDER BY (data.a, id) SETTINGS index_granularity = 1;
INSERT INTO t_alter_key_sub SELECT * FROM format(RowBinary, 'id UInt32, data JSON(a Date32)', unhex('0000000001016100000000010000000101610100000002000000010161ffffff7f'));
SELECT 'typed Date32 path, other paths change -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(a Date32, c String); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(toInt32(k)) FROM (SELECT data.a AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

-- A key subcolumn 31 levels deep: checking the ALTER must not take time exponential in the depth.
CREATE TABLE t_alter_key_sub (id UInt32, c Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Int64)))))))))))))))))))))))))))))), b UInt32)) ENGINE = MergeTree ORDER BY (c.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a, id) SETTINGS index_granularity = 1;
INSERT INTO t_alter_key_sub (id) VALUES (0);
SELECT 'key subcolumn 31 levels deep, a sibling changes -> allow:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN c Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Tuple(a Int64)))))))))))))))))))))))))))))), b UInt64);
SELECT toTypeName(c.b), c.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a.a FROM t_alter_key_sub;
DROP TABLE t_alter_key_sub;

-- The sizes of an array are not a subcolumn of its elements, even when the elements have a subcolumn of the same name.
CREATE TABLE t_alter_key_sub (id UInt32, a Array(JSON(x Int64))) ENGINE = MergeTree ORDER BY (a.size0, id) SETTINGS index_granularity = 1;
INSERT INTO t_alter_key_sub VALUES (0, ['{"x" : 1}']), (1, []), (2, ['{"x" : 1}', '{"x" : 2}']);
SELECT 'Array(JSON) sizes, the JSON changes -> allow:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN a Array(JSON(x Int64, y String));
SELECT 'key subcolumn values:', groupArray(k) FROM (SELECT a.size0 AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

CREATE TABLE t_alter_key_sub (id UInt32, a Array(Tuple(size0 UInt8, x Int64))) ENGINE = MergeTree ORDER BY (a.size0, id) SETTINGS index_granularity = 1;
INSERT INTO t_alter_key_sub VALUES (0, [(5, 1)]), (1, []), (2, [(5, 1), (5, 2)]);
SELECT 'Array(Tuple(size0 ...)) sizes, the element size0 changes -> allow:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN a Array(Tuple(size0 UInt16, x Int64));
SELECT 'key subcolumn values:', groupArray(k) FROM (SELECT a.size0 AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

CREATE TABLE t_alter_key_sub (id UInt32, a Array(Array(JSON(x Int64)))) ENGINE = MergeTree ORDER BY (a.size1, id) SETTINGS index_granularity = 1;
INSERT INTO t_alter_key_sub VALUES (0, [['{"x" : 1}']]), (1, [[]]), (2, [['{"x" : 1}', '{"x" : 2}']]);
SELECT 'Array(Array(JSON)) inner sizes, the JSON changes -> allow:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN a Array(Array(JSON(x Int64, y String)));
SELECT 'key subcolumn values:', groupArray(k) FROM (SELECT a.size1 AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

-- A geometry converted to its collection type becomes a collection of one, so the sizes change.
CREATE TABLE t_alter_key_sub (id UInt32, a LineString) ENGINE = MergeTree PARTITION BY a.size0 ORDER BY id;
INSERT INTO t_alter_key_sub VALUES (0, [(0, 0), (1, 1)]), (1, [(0, 0), (1, 1), (2, 2)]);
SELECT 'LineString sizes in the partition key, LineString -> MultiLineString -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN a MultiLineString; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(k) FROM (SELECT a.size0 AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

CREATE TABLE t_alter_key_sub (id UInt32, a Array(LineString)) ENGINE = MergeTree PARTITION BY a.size1 ORDER BY id;
INSERT INTO t_alter_key_sub VALUES (0, [[(0, 0), (1, 1)]]), (1, [[(0, 0), (1, 1), (2, 2)]]);
SELECT 'Array(LineString) inner sizes in the partition key, Array(LineString) -> Array(MultiLineString) -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN a Array(MultiLineString); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(k) FROM (SELECT a.size1 AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

CREATE TABLE t_alter_key_sub (id UInt32, a Array(LineString)) ENGINE = MergeTree PARTITION BY a.size0 ORDER BY id;
INSERT INTO t_alter_key_sub VALUES (0, [[(0, 0), (1, 1)]]), (1, [[(0, 0)], [(1, 1)]]);
SELECT 'Array(LineString) outer sizes in the partition key, Array(LineString) -> Array(MultiLineString) -> allow:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN a Array(MultiLineString);
SELECT 'key subcolumn values:', groupArray(k) FROM (SELECT a.size0 AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

CREATE TABLE t_alter_key_sub (id UInt32, a Array(Tuple(arr Array(JSON(x Int64)), b UInt8))) ENGINE = MergeTree ORDER BY (a.arr.size1, id) SETTINGS index_granularity = 1;
INSERT INTO t_alter_key_sub VALUES (0, [(['{"x" : 1}'], 1)]), (1, [([], 2)]), (2, [(['{"x" : 1}', '{"x" : 2}'], 3)]);
SELECT 'Array(Tuple(arr Array(JSON))) inner sizes, the JSON changes -> allow:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN a Array(Tuple(arr Array(JSON(x Int64, y String)), b UInt8));
SELECT 'key subcolumn values:', groupArray(k) FROM (SELECT a.arr.size1 AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

CREATE TABLE t_alter_key_sub (id UInt32, a Array(Tuple(g LineString))) ENGINE = MergeTree PARTITION BY a.g.size1 ORDER BY id;
INSERT INTO t_alter_key_sub VALUES (0, [tuple([(0, 0), (1, 1)])]), (1, [tuple([(0, 0), (1, 1), (2, 2)])]);
SELECT 'Array(Tuple(g LineString)) inner sizes in the partition key, LineString -> MultiLineString -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN a Array(Tuple(g MultiLineString)); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(k) FROM (SELECT a.g.size1 AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;

-- RapidJSON (allow_simdjson = 0) parses an integer wider than 64 bits as Float64.
CREATE TABLE t_alter_key_sub (id UInt32, data JSON(a UInt128)) ENGINE = MergeTree PARTITION BY data.a ORDER BY id;
INSERT INTO t_alter_key_sub VALUES (0, '{"a" : "18446744073709551617"}'), (1, '{"a" : "18446744073709551618"}');
SELECT 'typed UInt128 path in the partition key, other paths change -> reject:';
ALTER TABLE t_alter_key_sub MODIFY COLUMN data JSON(a UInt128, c String); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT 'key subcolumn values:', groupArray(k) FROM (SELECT data.a AS k FROM t_alter_key_sub ORDER BY id);
DROP TABLE t_alter_key_sub;
