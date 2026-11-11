-- { echo }

-- The index must hash the value the array-search function actually compares. Cells either compare
-- the keyed answer against an unindexed oracle, assert a granule reduction, or pin the rows the
-- index selects; every reference row answers 1, so no expected value is baked in.

DROP TABLE IF EXISTS o_str;
DROP TABLE IF EXISTS k_str;
DROP TABLE IF EXISTS o_fs3;
DROP TABLE IF EXISTS k_fs3;
DROP TABLE IF EXISTS o_lcstr;
DROP TABLE IF EXISTS k_lcstr;
DROP TABLE IF EXISTS o_lcfs3;
DROP TABLE IF EXISTS k_lcfs3;
DROP TABLE IF EXISTS o_lcnstr;
DROP TABLE IF EXISTS k_lcnstr;
DROP TABLE IF EXISTS o_lcnfs3;
DROP TABLE IF EXISTS k_lcnfs3;

CREATE TABLE o_str (id UInt64, v Array(String)) ENGINE = Log;
CREATE TABLE k_str (id UInt64, v Array(String), INDEX idx v TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO o_str VALUES (0,['V0']),(1,['V0\0']),(2,['V0\0\0']),(3,['X']);
INSERT INTO k_str VALUES (0,['V0']),(1,['V0\0']),(2,['V0\0\0']),(3,['X']);

CREATE TABLE o_fs3 (id UInt64, v Array(FixedString(3))) ENGINE = Log;
CREATE TABLE k_fs3 (id UInt64, v Array(FixedString(3)), INDEX idx v TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO o_fs3 VALUES (0,['V0']),(1,['V0A']),(2,['XYZ']),(3,['ZZZ']);
INSERT INTO k_fs3 VALUES (0,['V0']),(1,['V0A']),(2,['XYZ']),(3,['ZZZ']);

CREATE TABLE o_lcstr (id UInt64, v Array(LowCardinality(String))) ENGINE = Log;
CREATE TABLE k_lcstr (id UInt64, v Array(LowCardinality(String)), INDEX idx v TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO o_lcstr VALUES (0,['V0']),(1,['V0\0']),(2,['V0\0\0']),(3,['X']);
INSERT INTO k_lcstr VALUES (0,['V0']),(1,['V0\0']),(2,['V0\0\0']),(3,['X']);

CREATE TABLE o_lcfs3 (id UInt64, v Array(LowCardinality(FixedString(3)))) ENGINE = Log;
CREATE TABLE k_lcfs3 (id UInt64, v Array(LowCardinality(FixedString(3))), INDEX idx v TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO o_lcfs3 VALUES (0,['V0']),(1,['V0A']),(2,['XYZ']),(3,['ZZZ']);
INSERT INTO k_lcfs3 VALUES (0,['V0']),(1,['V0A']),(2,['XYZ']),(3,['ZZZ']);

CREATE TABLE o_lcnstr (id UInt64, v Array(LowCardinality(Nullable(String)))) ENGINE = Log;
CREATE TABLE k_lcnstr (id UInt64, v Array(LowCardinality(Nullable(String))), INDEX idx v TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO o_lcnstr VALUES (0,['V0']),(1,['V0\0']),(2,['V0\0\0']),(3,['X']);
INSERT INTO k_lcnstr VALUES (0,['V0']),(1,['V0\0']),(2,['V0\0\0']),(3,['X']);

CREATE TABLE o_lcnfs3 (id UInt64, v Array(LowCardinality(Nullable(FixedString(3))))) ENGINE = Log;
CREATE TABLE k_lcnfs3 (id UInt64, v Array(LowCardinality(Nullable(FixedString(3)))), INDEX idx v TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO o_lcnfs3 VALUES (0,['V0']),(1,['V0A']),(2,['XYZ']);
INSERT INTO k_lcnfs3 VALUES (0,['V0']),(1,['V0A']),(2,['XYZ']);

-- `Array(String)`: `hasAny`/`hasAll` cast both arrays to the common type, so they compare the
-- unpadded value.
SELECT 'str hasAny FS3', (SELECT count() FROM o_str WHERE hasAny(v,[toFixedString('V0',3)])) = (SELECT count() FROM k_str WHERE hasAny(v,[toFixedString('V0',3)]));
SELECT 'str hasAny FS5', (SELECT count() FROM o_str WHERE hasAny(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_str WHERE hasAny(v,[toFixedString('V0',5)]));
SELECT 'str hasAll FS3', (SELECT count() FROM o_str WHERE hasAll(v,[toFixedString('V0',3)])) = (SELECT count() FROM k_str WHERE hasAll(v,[toFixedString('V0',3)]));
SELECT 'str hasAll FS5', (SELECT count() FROM o_str WHERE hasAll(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_str WHERE hasAll(v,[toFixedString('V0',5)]));

-- `Array(String)`: `has`/`indexOf` take `executeString`, which compares the raw padded bytes. Must
-- not change.
SELECT 'str has Str', (SELECT count() FROM o_str WHERE has(v,'V0')) = (SELECT count() FROM k_str WHERE has(v,'V0'));
SELECT 'str has FS2', (SELECT count() FROM o_str WHERE has(v,toFixedString('V0',2))) = (SELECT count() FROM k_str WHERE has(v,toFixedString('V0',2)));
SELECT 'str has FS3', (SELECT count() FROM o_str WHERE has(v,toFixedString('V0',3))) = (SELECT count() FROM k_str WHERE has(v,toFixedString('V0',3)));
SELECT 'str has FS5', (SELECT count() FROM o_str WHERE has(v,toFixedString('V0',5))) = (SELECT count() FROM k_str WHERE has(v,toFixedString('V0',5)));
SELECT 'str indexOf Str', (SELECT count() FROM o_str WHERE indexOf(v,'V0') = 1) = (SELECT count() FROM k_str WHERE indexOf(v,'V0') = 1);
SELECT 'str indexOf FS3', (SELECT count() FROM o_str WHERE indexOf(v,toFixedString('V0',3)) = 1) = (SELECT count() FROM k_str WHERE indexOf(v,toFixedString('V0',3)) = 1);
SELECT 'str hasAny Str', (SELECT count() FROM o_str WHERE hasAny(v,['V0'])) = (SELECT count() FROM k_str WHERE hasAny(v,['V0']));
SELECT 'str hasAny FS2', (SELECT count() FROM o_str WHERE hasAny(v,[toFixedString('V0',2)])) = (SELECT count() FROM k_str WHERE hasAny(v,[toFixedString('V0',2)]));
SELECT 'str hasAll Str', (SELECT count() FROM o_str WHERE hasAll(v,['V0'])) = (SELECT count() FROM k_str WHERE hasAll(v,['V0']));

-- `Array(FixedString(3))`: a wider constant is stripped then re-encoded into the element width, so it
-- matches exactly. Before the fix `has`/`indexOf` answered 0 and `hasAny`/`hasAll` raised `Code 131`.
SELECT 'fs3 has FS5', (SELECT count() FROM o_fs3 WHERE has(v,toFixedString('V0',5))) = (SELECT count() FROM k_fs3 WHERE has(v,toFixedString('V0',5)));
SELECT 'fs3 indexOf FS5', (SELECT count() FROM o_fs3 WHERE indexOf(v,toFixedString('V0',5)) = 1) = (SELECT count() FROM k_fs3 WHERE indexOf(v,toFixedString('V0',5)) = 1);
SELECT 'fs3 hasAny FS5', (SELECT count() FROM o_fs3 WHERE hasAny(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_fs3 WHERE hasAny(v,[toFixedString('V0',5)]));
SELECT 'fs3 hasAll FS5', (SELECT count() FROM o_fs3 WHERE hasAll(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_fs3 WHERE hasAll(v,[toFixedString('V0',5)]));
SELECT 'fs3 has Str', (SELECT count() FROM o_fs3 WHERE has(v,'V0')) = (SELECT count() FROM k_fs3 WHERE has(v,'V0'));
SELECT 'fs3 has FS2', (SELECT count() FROM o_fs3 WHERE has(v,toFixedString('V0',2))) = (SELECT count() FROM k_fs3 WHERE has(v,toFixedString('V0',2)));
SELECT 'fs3 has FS3', (SELECT count() FROM o_fs3 WHERE has(v,toFixedString('V0',3))) = (SELECT count() FROM k_fs3 WHERE has(v,toFixedString('V0',3)));
SELECT 'fs3 indexOf FS3', (SELECT count() FROM o_fs3 WHERE indexOf(v,toFixedString('V0',3)) = 1) = (SELECT count() FROM k_fs3 WHERE indexOf(v,toFixedString('V0',3)) = 1);
SELECT 'fs3 hasAny FS3', (SELECT count() FROM o_fs3 WHERE hasAny(v,[toFixedString('V0',3)])) = (SELECT count() FROM k_fs3 WHERE hasAny(v,[toFixedString('V0',3)]));
SELECT 'fs3 hasAll FS3', (SELECT count() FROM o_fs3 WHERE hasAll(v,[toFixedString('V0',3)])) = (SELECT count() FROM k_fs3 WHERE hasAll(v,[toFixedString('V0',3)]));

-- A constant that does not fit the element width even after the padding is stripped: `WXYZ` is 4
-- bytes against `FixedString(3)`, so the second hop throws and analysis must decline. The engine has
-- an answer here without any index, so a decline must leave that answer intact rather than
-- propagating the error. This is the scalar twin of the batched case pinned further down.
SELECT 'fs3 has unrepresentable FS5', (SELECT count() FROM o_fs3 WHERE has(v,toFixedString('WXYZ',5))) = (SELECT count() FROM k_fs3 WHERE has(v,toFixedString('WXYZ',5)));
SELECT 'fs3 indexOf unrepresentable FS5', (SELECT count() FROM o_fs3 WHERE indexOf(v,toFixedString('WXYZ',5)) = 1) = (SELECT count() FROM k_fs3 WHERE indexOf(v,toFixedString('WXYZ',5)) = 1);
SELECT 'fs3 has unrepresentable FS5 is 0', (SELECT count() FROM k_fs3 WHERE has(v,toFixedString('WXYZ',5))) = 0;

-- `Array(LowCardinality(String))`: every predicate coerces here, so every `FixedString` constant was
-- wrong.
SELECT 'lcstr has FS3', (SELECT count() FROM o_lcstr WHERE has(v,toFixedString('V0',3))) = (SELECT count() FROM k_lcstr WHERE has(v,toFixedString('V0',3)));
SELECT 'lcstr has FS5', (SELECT count() FROM o_lcstr WHERE has(v,toFixedString('V0',5))) = (SELECT count() FROM k_lcstr WHERE has(v,toFixedString('V0',5)));
SELECT 'lcstr indexOf FS3', (SELECT count() FROM o_lcstr WHERE indexOf(v,toFixedString('V0',3)) = 1) = (SELECT count() FROM k_lcstr WHERE indexOf(v,toFixedString('V0',3)) = 1);
SELECT 'lcstr indexOf FS5', (SELECT count() FROM o_lcstr WHERE indexOf(v,toFixedString('V0',5)) = 1) = (SELECT count() FROM k_lcstr WHERE indexOf(v,toFixedString('V0',5)) = 1);
SELECT 'lcstr hasAny FS3', (SELECT count() FROM o_lcstr WHERE hasAny(v,[toFixedString('V0',3)])) = (SELECT count() FROM k_lcstr WHERE hasAny(v,[toFixedString('V0',3)]));
SELECT 'lcstr hasAny FS5', (SELECT count() FROM o_lcstr WHERE hasAny(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_lcstr WHERE hasAny(v,[toFixedString('V0',5)]));
SELECT 'lcstr hasAll FS3', (SELECT count() FROM o_lcstr WHERE hasAll(v,[toFixedString('V0',3)])) = (SELECT count() FROM k_lcstr WHERE hasAll(v,[toFixedString('V0',3)]));
SELECT 'lcstr hasAll FS5', (SELECT count() FROM o_lcstr WHERE hasAll(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_lcstr WHERE hasAll(v,[toFixedString('V0',5)]));
SELECT 'lcstr has Str', (SELECT count() FROM o_lcstr WHERE has(v,'V0')) = (SELECT count() FROM k_lcstr WHERE has(v,'V0'));
SELECT 'lcstr has FS2', (SELECT count() FROM o_lcstr WHERE has(v,toFixedString('V0',2))) = (SELECT count() FROM k_lcstr WHERE has(v,toFixedString('V0',2)));
SELECT 'lcstr hasAny Str', (SELECT count() FROM o_lcstr WHERE hasAny(v,['V0'])) = (SELECT count() FROM k_lcstr WHERE hasAny(v,['V0']));

-- `Array(LowCardinality(FixedString(3)))`: `hasAny`/`hasAll` cast to the supertype and legitimately
-- match.
SELECT 'lcfs3 hasAny FS5', (SELECT count() FROM o_lcfs3 WHERE hasAny(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_lcfs3 WHERE hasAny(v,[toFixedString('V0',5)]));
SELECT 'lcfs3 hasAll FS5', (SELECT count() FROM o_lcfs3 WHERE hasAll(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_lcfs3 WHERE hasAll(v,[toFixedString('V0',5)]));
SELECT 'lcfs3 has FS3', (SELECT count() FROM o_lcfs3 WHERE has(v,toFixedString('V0',3))) = (SELECT count() FROM k_lcfs3 WHERE has(v,toFixedString('V0',3)));
SELECT 'lcfs3 hasAny FS3', (SELECT count() FROM o_lcfs3 WHERE hasAny(v,[toFixedString('V0',3)])) = (SELECT count() FROM k_lcfs3 WHERE hasAny(v,[toFixedString('V0',3)]));

-- `LowCardinality(Nullable(...))` is the one element type carrying both wrappers, so
-- `getPrimitiveType` strips both. The mode is per-PREDICATE, not per-type: `has`/`indexOf` see the raw
-- `LowCardinality` and take the Direct mode, while `hasAny`/`hasAll` route through the batched helper,
-- whose two hops are always Supertype. `bloom_filter` accepts this type because the array's data column
-- is a `ColumnLowCardinality`, not a `ColumnNullable`, and two existing tests already index it.
SELECT 'lcnstr has FS3', (SELECT count() FROM o_lcnstr WHERE has(v,toFixedString('V0',3))) = (SELECT count() FROM k_lcnstr WHERE has(v,toFixedString('V0',3)));
SELECT 'lcnstr indexOf FS3', (SELECT count() FROM o_lcnstr WHERE indexOf(v,toFixedString('V0',3)) = 1) = (SELECT count() FROM k_lcnstr WHERE indexOf(v,toFixedString('V0',3)) = 1);
SELECT 'lcnstr hasAny FS3', (SELECT count() FROM o_lcnstr WHERE hasAny(v,[toFixedString('V0',3)])) = (SELECT count() FROM k_lcnstr WHERE hasAny(v,[toFixedString('V0',3)]));
SELECT 'lcnstr hasAll FS3', (SELECT count() FROM o_lcnstr WHERE hasAll(v,[toFixedString('V0',3)])) = (SELECT count() FROM k_lcnstr WHERE hasAll(v,[toFixedString('V0',3)]));
SELECT 'lcnstr hasAny FS5', (SELECT count() FROM o_lcnstr WHERE hasAny(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_lcnstr WHERE hasAny(v,[toFixedString('V0',5)]));
SELECT 'lcnstr has Str', (SELECT count() FROM o_lcnstr WHERE has(v,'V0')) = (SELECT count() FROM k_lcnstr WHERE has(v,'V0'));
SELECT 'lcnfs3 hasAny FS5', (SELECT count() FROM o_lcnfs3 WHERE hasAny(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_lcnfs3 WHERE hasAny(v,[toFixedString('V0',5)]));
SELECT 'lcnfs3 hasAll FS5', (SELECT count() FROM o_lcnfs3 WHERE hasAll(v,[toFixedString('V0',5)])) = (SELECT count() FROM k_lcnfs3 WHERE hasAll(v,[toFixedString('V0',5)]));
SELECT 'lcnfs3 has FS3', (SELECT count() FROM o_lcnfs3 WHERE has(v,toFixedString('V0',3))) = (SELECT count() FROM k_lcnfs3 WHERE has(v,toFixedString('V0',3)));
-- Adding `Nullable` inside `LowCardinality` changes the wider-constant outcome: `Array(LowCardinality(FixedString(3)))`
-- raises `Code 131` at `has(v, toFixedString('V0',5))` (pinned at the `serverError` cells below), while the
-- nullable element absorbs the failed cast and answers 0 on both arms. Must not change.
SELECT 'lcnfs3 has FS5', (SELECT count() FROM o_lcnfs3 WHERE has(v,toFixedString('V0',5))) = (SELECT count() FROM k_lcnfs3 WHERE has(v,toFixedString('V0',5)));

-- `has`/`indexOf` over a `LowCardinality` element cast the constant straight to the dictionary type,
-- which overflows for a wider constant. The engine raises this with no index at all, so the index must
-- decline rather than answer 0.
SELECT count() FROM o_lcfs3 WHERE has(v,toFixedString('V0',5)); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT count() FROM k_lcfs3 WHERE has(v,toFixedString('V0',5)); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT count() FROM o_lcfs3 WHERE indexOf(v,toFixedString('V0',5)) = 1; -- { serverError TOO_LARGE_STRING_SIZE }
SELECT count() FROM k_lcfs3 WHERE indexOf(v,toFixedString('V0',5)) = 1; -- { serverError TOO_LARGE_STRING_SIZE }

-- A `NULL` constant has no representation in the index domain, so analysis must decline instead of
-- materializing it. The constant must be a TYPED null: a bare `NULL` is `Nullable(Nothing)`, which
-- fails the string test and declines earlier, so a bare-literal cell would not reach the coercion at
-- all. `RPNBuilder` keeps the constant type `Nullable` only when the value IS Null, so this is the
-- exact pair that arrives. Every cell answers 0; asserting keyed == oracle is what catches a throw.
SELECT 'null str has NStr', (SELECT count() FROM o_str WHERE has(v,CAST(NULL AS Nullable(String)))) = (SELECT count() FROM k_str WHERE has(v,CAST(NULL AS Nullable(String))));
SELECT 'null str indexOf NStr', (SELECT count() FROM o_str WHERE indexOf(v,CAST(NULL AS Nullable(String))) = 1) = (SELECT count() FROM k_str WHERE indexOf(v,CAST(NULL AS Nullable(String))) = 1);
SELECT 'null str hasAny NStr', (SELECT count() FROM o_str WHERE hasAny(v,[CAST(NULL AS Nullable(String))])) = (SELECT count() FROM k_str WHERE hasAny(v,[CAST(NULL AS Nullable(String))]));
SELECT 'null str hasAll NStr', (SELECT count() FROM o_str WHERE hasAll(v,[CAST(NULL AS Nullable(String))])) = (SELECT count() FROM k_str WHERE hasAll(v,[CAST(NULL AS Nullable(String))]));
SELECT 'null fs3 has NStr', (SELECT count() FROM o_fs3 WHERE has(v,CAST(NULL AS Nullable(String)))) = (SELECT count() FROM k_fs3 WHERE has(v,CAST(NULL AS Nullable(String))));
SELECT 'null fs3 has NFS3', (SELECT count() FROM o_fs3 WHERE has(v,CAST(NULL AS Nullable(FixedString(3))))) = (SELECT count() FROM k_fs3 WHERE has(v,CAST(NULL AS Nullable(FixedString(3)))));
SELECT 'null fs3 indexOf NStr', (SELECT count() FROM o_fs3 WHERE indexOf(v,CAST(NULL AS Nullable(String))) = 1) = (SELECT count() FROM k_fs3 WHERE indexOf(v,CAST(NULL AS Nullable(String))) = 1);
SELECT 'null fs3 indexOf NFS3', (SELECT count() FROM o_fs3 WHERE indexOf(v,CAST(NULL AS Nullable(FixedString(3)))) = 1) = (SELECT count() FROM k_fs3 WHERE indexOf(v,CAST(NULL AS Nullable(FixedString(3)))) = 1);
SELECT 'null fs3 hasAny NFS3', (SELECT count() FROM o_fs3 WHERE hasAny(v,[CAST(NULL AS Nullable(FixedString(3)))])) = (SELECT count() FROM k_fs3 WHERE hasAny(v,[CAST(NULL AS Nullable(FixedString(3)))]));
SELECT 'null fs3 hasAll NFS3', (SELECT count() FROM o_fs3 WHERE hasAll(v,[CAST(NULL AS Nullable(FixedString(3)))])) = (SELECT count() FROM k_fs3 WHERE hasAll(v,[CAST(NULL AS Nullable(FixedString(3)))]));
SELECT 'null lcstr has NStr', (SELECT count() FROM o_lcstr WHERE has(v,CAST(NULL AS Nullable(String)))) = (SELECT count() FROM k_lcstr WHERE has(v,CAST(NULL AS Nullable(String))));
SELECT 'null lcstr has NFS3', (SELECT count() FROM o_lcstr WHERE has(v,CAST(NULL AS Nullable(FixedString(3))))) = (SELECT count() FROM k_lcstr WHERE has(v,CAST(NULL AS Nullable(FixedString(3)))));
SELECT 'null lcstr indexOf NStr', (SELECT count() FROM o_lcstr WHERE indexOf(v,CAST(NULL AS Nullable(String))) = 1) = (SELECT count() FROM k_lcstr WHERE indexOf(v,CAST(NULL AS Nullable(String))) = 1);
SELECT 'null lcstr indexOf NFS3', (SELECT count() FROM o_lcstr WHERE indexOf(v,CAST(NULL AS Nullable(FixedString(3)))) = 1) = (SELECT count() FROM k_lcstr WHERE indexOf(v,CAST(NULL AS Nullable(FixedString(3)))) = 1);
SELECT 'null lcstr hasAny NStr', (SELECT count() FROM o_lcstr WHERE hasAny(v,[CAST(NULL AS Nullable(String))])) = (SELECT count() FROM k_lcstr WHERE hasAny(v,[CAST(NULL AS Nullable(String))]));
SELECT 'null lcstr hasAll NStr', (SELECT count() FROM o_lcstr WHERE hasAll(v,[CAST(NULL AS Nullable(String))])) = (SELECT count() FROM k_lcstr WHERE hasAll(v,[CAST(NULL AS Nullable(String))]));
SELECT 'null lcfs3 has NStr', (SELECT count() FROM o_lcfs3 WHERE has(v,CAST(NULL AS Nullable(String)))) = (SELECT count() FROM k_lcfs3 WHERE has(v,CAST(NULL AS Nullable(String))));
SELECT 'null lcfs3 has NFS3', (SELECT count() FROM o_lcfs3 WHERE has(v,CAST(NULL AS Nullable(FixedString(3))))) = (SELECT count() FROM k_lcfs3 WHERE has(v,CAST(NULL AS Nullable(FixedString(3)))));
SELECT 'null lcfs3 indexOf NStr', (SELECT count() FROM o_lcfs3 WHERE indexOf(v,CAST(NULL AS Nullable(String))) = 1) = (SELECT count() FROM k_lcfs3 WHERE indexOf(v,CAST(NULL AS Nullable(String))) = 1);
SELECT 'null lcfs3 indexOf NFS3', (SELECT count() FROM o_lcfs3 WHERE indexOf(v,CAST(NULL AS Nullable(FixedString(3)))) = 1) = (SELECT count() FROM k_lcfs3 WHERE indexOf(v,CAST(NULL AS Nullable(FixedString(3)))) = 1);
SELECT 'null lcfs3 hasAny NFS3', (SELECT count() FROM o_lcfs3 WHERE hasAny(v,[CAST(NULL AS Nullable(FixedString(3)))])) = (SELECT count() FROM k_lcfs3 WHERE hasAny(v,[CAST(NULL AS Nullable(FixedString(3)))]));
SELECT 'null lcfs3 hasAll NFS3', (SELECT count() FROM o_lcfs3 WHERE hasAll(v,[CAST(NULL AS Nullable(FixedString(3)))])) = (SELECT count() FROM k_lcfs3 WHERE hasAll(v,[CAST(NULL AS Nullable(FixedString(3)))]));

DROP TABLE o_str;
DROP TABLE k_str;
DROP TABLE o_fs3;
DROP TABLE k_fs3;
DROP TABLE o_lcstr;
DROP TABLE k_lcstr;
DROP TABLE o_lcfs3;
DROP TABLE k_lcfs3;
DROP TABLE o_lcnstr;
DROP TABLE k_lcnstr;
DROP TABLE o_lcnfs3;
DROP TABLE k_lcnfs3;
