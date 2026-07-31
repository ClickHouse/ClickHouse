-- The oracle is `=` itself, evaluated element-wise with the rewrite to `has` disabled, so it
-- cannot be produced by the code under test. Every membership answer must equal it.
SET optimize_rewrite_array_exists_to_has = 0;

DROP TABLE IF EXISTS t_str;
DROP TABLE IF EXISTS t_lc;
DROP TABLE IF EXISTS t_lc_null;
DROP TABLE IF EXISTS t_null;
DROP TABLE IF EXISTS t_both;
DROP TABLE IF EXISTS t_both_lc;
DROP TABLE IF EXISTS t_fs4;
DROP TABLE IF EXISTS t_lc_fs3;
DROP TABLE IF EXISTS t_map;
DROP TABLE IF EXISTS t_map_lc;
DROP TABLE IF EXISTS t_map_val;
DROP TABLE IF EXISTS t_map_val_lc;

-- id0 and id1 are two members of the equivalence class of toFixedString('V0', 3):
-- string-family equality is zero-padded, so both must match.
CREATE TABLE t_str     (id UInt64, v Array(String)) ENGINE = Memory;
CREATE TABLE t_lc      (id UInt64, v Array(LowCardinality(String))) ENGINE = Memory;
CREATE TABLE t_lc_null (id UInt64, v Array(LowCardinality(Nullable(String)))) ENGINE = Memory;
CREATE TABLE t_null    (id UInt64, v Array(Nullable(String))) ENGINE = Memory;
INSERT INTO t_str     VALUES (0, ['V0']), (1, ['V0\0']), (2, ['X']);
INSERT INTO t_lc      VALUES (0, ['V0']), (1, ['V0\0']), (2, ['X']);
INSERT INTO t_lc_null VALUES (0, ['V0']), (1, ['V0\0']), (2, ['X']);
INSERT INTO t_null    VALUES (0, ['V0']), (1, ['V0\0']), (2, ['X']);

SELECT '-- match set equals the element-wise `=` oracle';
SELECT 'String',                     groupArray(id), (SELECT groupArray(id) FROM t_str     WHERE arrayExists(x -> x = toFixedString('V0', 3), v)) FROM t_str     WHERE has(v, toFixedString('V0', 3));
SELECT 'LowCardinality',             groupArray(id), (SELECT groupArray(id) FROM t_lc      WHERE arrayExists(x -> x = toFixedString('V0', 3), v)) FROM t_lc      WHERE has(v, toFixedString('V0', 3));
SELECT 'LowCardinality(Nullable)',   groupArray(id), (SELECT groupArray(id) FROM t_lc_null WHERE arrayExists(x -> x = toFixedString('V0', 3), v)) FROM t_lc_null WHERE has(v, toFixedString('V0', 3));
SELECT 'Nullable',                   groupArray(id), (SELECT groupArray(id) FROM t_null    WHERE arrayExists(x -> x = toFixedString('V0', 3), v)) FROM t_null    WHERE has(v, toFixedString('V0', 3));
SELECT 'indexOf String',             groupArray(id) FROM t_str WHERE indexOf(v, toFixedString('V0', 3)) > 0;
SELECT 'indexOf LowCardinality',     groupArray(id) FROM t_lc  WHERE indexOf(v, toFixedString('V0', 3)) > 0;
SELECT 'countEqual String',          groupArray(id) FROM t_str WHERE countEqual(v, toFixedString('V0', 3)) > 0;
SELECT 'countEqual LowCardinality',  groupArray(id) FROM t_lc  WHERE countEqual(v, toFixedString('V0', 3)) > 0;

SELECT '-- a needle wider than the stored values still matches';
SELECT 'String',         groupArray(id), (SELECT groupArray(id) FROM t_str WHERE arrayExists(x -> x = toFixedString('V0', 5), v)) FROM t_str WHERE has(v, toFixedString('V0', 5));
SELECT 'LowCardinality', groupArray(id), (SELECT groupArray(id) FROM t_lc  WHERE arrayExists(x -> x = toFixedString('V0', 5), v)) FROM t_lc  WHERE has(v, toFixedString('V0', 5));

-- One row holding BOTH class members. No path that reduces the needle to a single value can
-- answer these, so they pin the fix at the element-wise layer.
SELECT '-- multiplicity and position with both class members in one row';
CREATE TABLE t_both    (v Array(String)) ENGINE = Memory;
CREATE TABLE t_both_lc (v Array(LowCardinality(String))) ENGINE = Memory;
INSERT INTO t_both    VALUES (['V0', 'V0\0']);
INSERT INTO t_both_lc VALUES (['V0', 'V0\0']);
SELECT 'String',         countEqual(v, toFixedString('V0', 3)), indexOf(v, toFixedString('V0', 3)), arraySum(arrayMap(y -> toUInt8(y = toFixedString('V0', 3)), v)) FROM t_both;
SELECT 'LowCardinality', countEqual(v, toFixedString('V0', 3)), indexOf(v, toFixedString('V0', 3)), arraySum(arrayMap(y -> toUInt8(y = toFixedString('V0', 3)), v)) FROM t_both_lc;

SELECT '-- constant array, compared as Fields';
SELECT has(['V0', 'V0\0'], toFixedString('V0', 3)), indexOf(['V0', 'V0\0'], toFixedString('V0', 3)), countEqual(['V0', 'V0\0'], toFixedString('V0', 3));
SELECT has(['V0', 'V0\0'], toFixedString('V0', 5)), has([toFixedString('V0', 4)], 'V0'), has([toFixedString('V0', 3)], 'V0\0\0\0');
SELECT 'per row', has(['V0', 'V0\0'], toFixedString('V0', 3)), indexOf(['V0', 'V0\0'], toFixedString('V0', 3)) FROM t_str ORDER BY id;

SELECT '-- Map keys take the same path';
CREATE TABLE t_map    (id UInt64, m Map(String, UInt8)) ENGINE = Memory;
CREATE TABLE t_map_lc (id UInt64, m Map(LowCardinality(String), UInt8)) ENGINE = Memory;
INSERT INTO t_map    VALUES (0, {'V0':1}), (1, {'V0\0':1}), (2, {'X':1});
INSERT INTO t_map_lc VALUES (0, {'V0':1}), (1, {'V0\0':1}), (2, {'X':1});
SELECT 'Map',                 groupArray(id) FROM t_map    WHERE has(m, toFixedString('V0', 3));
SELECT 'Map(LowCardinality)', groupArray(id) FROM t_map_lc WHERE has(m, toFixedString('V0', 3));
-- `has(Map, ...)` strips LowCardinality before dispatch, so it cannot reach the LowCardinality
-- shortcut. `mapContainsKey` and `mapContainsValue` go through the Map-to-array adapter, which keeps
-- LowCardinality, and select a different tuple element from each other, so all four are separate.
CREATE TABLE t_map_val    (id UInt64, m Map(UInt8, String)) ENGINE = Memory;
CREATE TABLE t_map_val_lc (id UInt64, m Map(UInt8, LowCardinality(String))) ENGINE = Memory;
INSERT INTO t_map_val    VALUES (0, {0:'V0'}), (1, {1:'V0\0'}), (2, {2:'X'});
INSERT INTO t_map_val_lc VALUES (0, {0:'V0'}), (1, {1:'V0\0'}), (2, {2:'X'});
SELECT 'mapContainsKey',                   groupArray(id), (SELECT groupArray(id) FROM t_map        WHERE arrayExists(x -> x = toFixedString('V0', 3), mapKeys(m)))   FROM t_map        WHERE mapContainsKey(m, toFixedString('V0', 3));
SELECT 'mapContainsKey LowCardinality',    groupArray(id), (SELECT groupArray(id) FROM t_map_lc     WHERE arrayExists(x -> x = toFixedString('V0', 3), mapKeys(m)))   FROM t_map_lc     WHERE mapContainsKey(m, toFixedString('V0', 3));
SELECT 'mapContainsValue',                 groupArray(id), (SELECT groupArray(id) FROM t_map_val    WHERE arrayExists(x -> x = toFixedString('V0', 3), mapValues(m))) FROM t_map_val    WHERE mapContainsValue(m, toFixedString('V0', 3));
SELECT 'mapContainsValue LowCardinality',  groupArray(id), (SELECT groupArray(id) FROM t_map_val_lc WHERE arrayExists(x -> x = toFixedString('V0', 3), mapValues(m))) FROM t_map_val_lc WHERE mapContainsValue(m, toFixedString('V0', 3));

SELECT '-- the rewrite of arrayExists to has must preserve results';
SELECT 'FixedString needle', (SELECT groupArray(id) FROM t_str WHERE arrayExists(x -> x = toFixedString('V0', 3), v) SETTINGS optimize_rewrite_array_exists_to_has = 1)
                          = (SELECT groupArray(id) FROM t_str WHERE arrayExists(x -> x = toFixedString('V0', 3), v) SETTINGS optimize_rewrite_array_exists_to_has = 0);
SELECT 'String needle',      (SELECT groupArray(id) FROM t_str WHERE arrayExists(x -> x = 'V0', v) SETTINGS optimize_rewrite_array_exists_to_has = 1)
                          = (SELECT groupArray(id) FROM t_str WHERE arrayExists(x -> x = 'V0', v) SETTINGS optimize_rewrite_array_exists_to_has = 0);

SELECT '-- must not regress: a String needle stays exact, FixedString elements keep working';
CREATE TABLE t_fs4    (id UInt64, v Array(FixedString(4))) ENGINE = Memory;
CREATE TABLE t_lc_fs3 (id UInt64, v Array(LowCardinality(FixedString(3)))) ENGINE = Memory;
INSERT INTO t_fs4    VALUES (0, ['V0']), (1, ['X']);
INSERT INTO t_lc_fs3 VALUES (0, ['V0']), (1, ['X']);
SELECT 'String needle on String',           groupArray(id), (SELECT groupArray(id) FROM t_str WHERE arrayExists(x -> x = 'V0', v)) FROM t_str WHERE has(v, 'V0');
SELECT 'String needle on LowCardinality',   groupArray(id), (SELECT groupArray(id) FROM t_lc  WHERE arrayExists(x -> x = 'V0', v)) FROM t_lc  WHERE has(v, 'V0');
SELECT 'FixedString(4) elements',           groupArray(id), (SELECT groupArray(id) FROM t_fs4 WHERE arrayExists(x -> x = toFixedString('V0', 3), v)) FROM t_fs4 WHERE has(v, toFixedString('V0', 3));
SELECT 'LowCardinality(FixedString(3))',    groupArray(id), (SELECT groupArray(id) FROM t_lc_fs3 WHERE arrayExists(x -> x = toFixedString('V0', 3), v)) FROM t_lc_fs3 WHERE has(v, toFixedString('V0', 3));
-- A needle wider than a FixedString element used to throw TOO_LARGE_STRING_SIZE here.
SELECT 'wider needle on LowCardinality(FixedString(3))', groupArray(id), (SELECT groupArray(id) FROM t_lc_fs3 WHERE arrayExists(x -> x = toFixedString('V0', 5), v)) FROM t_lc_fs3 WHERE has(v, toFixedString('V0', 5));
SELECT 'longer non-NUL String needle',      has(v, materialize('V0abc')) FROM t_fs4 WHERE id = 0;
SELECT 'NULL needle',                       groupArray(id) FROM t_lc_null WHERE has(v, NULL);

SELECT '-- boundary sizes, including needles that cross the 16 byte comparison window';
DROP TABLE IF EXISTS t_edge;
CREATE TABLE t_edge (id UInt64, v Array(String)) ENGINE = Memory;
INSERT INTO t_edge VALUES (0, ['']), (1, ['a']), (2, ['a\0']), (3, ['\0a']), (4, ['0123456789abcdef']), (5, ['0123456789abcde']);
SELECT 'empty value',       groupArray(id), (SELECT groupArray(id) FROM t_edge WHERE arrayExists(x -> x = toFixedString('', 1), v))                 FROM t_edge WHERE has(v, toFixedString('', 1));
SELECT 'one byte',          groupArray(id), (SELECT groupArray(id) FROM t_edge WHERE arrayExists(x -> x = toFixedString('a', 2), v))                FROM t_edge WHERE has(v, toFixedString('a', 2));
SELECT 'leading NUL',       groupArray(id), (SELECT groupArray(id) FROM t_edge WHERE arrayExists(x -> x = toFixedString('\0a', 2), v))              FROM t_edge WHERE has(v, toFixedString('\0a', 2));
SELECT 'exactly 16 bytes',  groupArray(id), (SELECT groupArray(id) FROM t_edge WHERE arrayExists(x -> x = toFixedString('0123456789abcdef', 16), v)) FROM t_edge WHERE has(v, toFixedString('0123456789abcdef', 16));
SELECT 'padded across 16',  groupArray(id), (SELECT groupArray(id) FROM t_edge WHERE arrayExists(x -> x = toFixedString('0123456789abcde', 16), v))  FROM t_edge WHERE has(v, toFixedString('0123456789abcde', 16));
SELECT 'padded to 40',      groupArray(id), (SELECT groupArray(id) FROM t_edge WHERE arrayExists(x -> x = toFixedString('0123456789abcde', 40), v))  FROM t_edge WHERE has(v, toFixedString('0123456789abcde', 40));
SELECT 'empty array',       has([]::Array(String), toFixedString('V0', 3));
DROP TABLE t_edge;

SELECT '-- the dictionary lookup must not stand in for float equality';
SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS t_f64_lc;
DROP TABLE IF EXISTS t_f64;
DROP TABLE IF EXISTS t_i64_lc;
DROP TABLE IF EXISTS t_f32_lc;
-- The zeros must be in separate rows: within one row INSERT normalises -0.0 to 0.0.
CREATE TABLE t_f64_lc (id UInt64, v Array(LowCardinality(Float64))) ENGINE = Memory;
CREATE TABLE t_f64    (id UInt64, v Array(Float64)) ENGINE = Memory;
INSERT INTO t_f64_lc VALUES (0, [0.0]), (1, [-0.0]), (2, [nan]);
INSERT INTO t_f64    VALUES (0, [0.0]), (1, [-0.0]), (2, [nan]);
SELECT 'has 0.0',    groupArray(id), (SELECT groupArray(id) FROM t_f64 WHERE arrayExists(x -> x = 0.0, v)), (SELECT groupArray(id) FROM t_f64 WHERE has(v, 0.0)) FROM t_f64_lc WHERE has(v, 0.0);
SELECT 'has -0.0',   groupArray(id), (SELECT groupArray(id) FROM t_f64 WHERE arrayExists(x -> x = -0.0, v)), (SELECT groupArray(id) FROM t_f64 WHERE has(v, -0.0)) FROM t_f64_lc WHERE has(v, -0.0);
SELECT 'has nan',    groupArray(id), (SELECT groupArray(id) FROM t_f64 WHERE arrayExists(x -> x = nan, v)), (SELECT groupArray(id) FROM t_f64 WHERE has(v, nan)) FROM t_f64_lc WHERE has(v, nan);
SELECT 'nan position and count', indexOf(v, nan), countEqual(v, nan) FROM t_f64_lc WHERE id = 2;

SELECT '-- a needle of a wider type must not be rounded onto a stored value';
CREATE TABLE t_i64_lc (id UInt64, v Array(LowCardinality(Int64))) ENGINE = Memory;
CREATE TABLE t_f32_lc (id UInt64, v Array(LowCardinality(Float32))) ENGINE = Memory;
INSERT INTO t_i64_lc VALUES (0, [1]), (1, [2]);
INSERT INTO t_f32_lc VALUES (0, [0.1]);
SELECT 'has 1.5',              groupArray(id), (SELECT groupArray(id) FROM t_i64_lc WHERE arrayExists(x -> x = 1.5, v)) FROM t_i64_lc WHERE has(v, 1.5);
SELECT 'has 2.5',              groupArray(id), (SELECT groupArray(id) FROM t_i64_lc WHERE arrayExists(x -> x = 2.5, v)) FROM t_i64_lc WHERE has(v, 2.5);
SELECT 'has toDecimal64(1.5)', groupArray(id) FROM t_i64_lc WHERE has(v, toDecimal64(1.5, 1));
SELECT 'Float32 element, Float64 needle', groupArray(id), (SELECT groupArray(id) FROM t_f32_lc WHERE arrayExists(x -> x = 0.1::Float64, v)) FROM t_f32_lc WHERE has(v, 0.1::Float64);
SELECT 'same type needle',     groupArray(id), (SELECT groupArray(id) FROM t_i64_lc WHERE arrayExists(x -> x = 1::Int64, v)) FROM t_i64_lc WHERE has(v, 1::Int64);

-- A negative needle against an unsigned element: no stored value can equal it, and the dictionary
-- must not match one by reinterpreting the bytes.
DROP TABLE IF EXISTS t_u8_lc;
DROP TABLE IF EXISTS t_u8;
CREATE TABLE t_u8_lc (id UInt64, v Array(LowCardinality(UInt8))) ENGINE = Memory;
CREATE TABLE t_u8    (id UInt64, v Array(UInt8)) ENGINE = Memory;
INSERT INTO t_u8_lc VALUES (0, [0, 255, 254]);
INSERT INTO t_u8    VALUES (0, [0, 255, 254]);
SELECT 'has -1 on UInt8',  groupArray(id), (SELECT groupArray(id) FROM t_u8 WHERE arrayExists(x -> x = -1, v)), (SELECT groupArray(id) FROM t_u8 WHERE has(v, -1)) FROM t_u8_lc WHERE has(v, -1);
SELECT 'has 255 on UInt8', groupArray(id), (SELECT groupArray(id) FROM t_u8 WHERE arrayExists(x -> x = 255, v)) FROM t_u8_lc WHERE has(v, 255);
DROP TABLE t_u8_lc;
DROP TABLE t_u8;

DROP TABLE t_str;
DROP TABLE t_lc;
DROP TABLE t_lc_null;
DROP TABLE t_null;
DROP TABLE t_both;
DROP TABLE t_both_lc;
DROP TABLE t_fs4;
DROP TABLE t_lc_fs3;
DROP TABLE t_map;
DROP TABLE t_map_lc;
DROP TABLE t_map_val;
DROP TABLE t_map_val_lc;
DROP TABLE t_f64_lc;
DROP TABLE t_f64;
DROP TABLE t_i64_lc;
DROP TABLE t_f32_lc;
