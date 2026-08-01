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

-- A nullable constant needle is the same constant FixedString argument up to nullability, so it must
-- give the same answers. It arrives wrapped as a constant nullable column, which is a shape the
-- string handlers do not recognize unless the wrapper is peeled off first.
SELECT '-- a nullable constant needle behaves like the non-nullable one';
SELECT 'Nullable(FixedString) String',                   groupArray(id), (SELECT groupArray(id) FROM t_str     WHERE arrayExists(x -> x = CAST(toFixedString('V0', 3) AS Nullable(FixedString(3))), v)) FROM t_str     WHERE has(v, CAST(toFixedString('V0', 3) AS Nullable(FixedString(3))));
SELECT 'Nullable(FixedString) LowCardinality',           groupArray(id), (SELECT groupArray(id) FROM t_lc      WHERE arrayExists(x -> x = CAST(toFixedString('V0', 3) AS Nullable(FixedString(3))), v)) FROM t_lc      WHERE has(v, CAST(toFixedString('V0', 3) AS Nullable(FixedString(3))));
SELECT 'Nullable(FixedString) Nullable',                 groupArray(id), (SELECT groupArray(id) FROM t_null    WHERE arrayExists(x -> x = CAST(toFixedString('V0', 3) AS Nullable(FixedString(3))), v)) FROM t_null    WHERE has(v, CAST(toFixedString('V0', 3) AS Nullable(FixedString(3))));
SELECT 'Nullable(FixedString) LowCardinality(Nullable)', groupArray(id), (SELECT groupArray(id) FROM t_lc_null WHERE arrayExists(x -> x = CAST(toFixedString('V0', 3) AS Nullable(FixedString(3))), v)) FROM t_lc_null WHERE has(v, CAST(toFixedString('V0', 3) AS Nullable(FixedString(3))));
SELECT 'Nullable(FixedString) multiplicity and position', countEqual(v, CAST(toFixedString('V0', 3) AS Nullable(FixedString(3)))), indexOf(v, CAST(toFixedString('V0', 3) AS Nullable(FixedString(3)))), arraySum(arrayMap(y -> toUInt8(assumeNotNull(y = CAST(toFixedString('V0', 3) AS Nullable(FixedString(3))))), v)), arrayFirstIndex(y -> assumeNotNull(y = CAST(toFixedString('V0', 3) AS Nullable(FixedString(3)))), v) FROM t_both;
-- Must not regress: peeling the wrapper must not make a String needle padded.
SELECT 'Nullable(String) needle',                        groupArray(id), (SELECT groupArray(id) FROM t_str WHERE arrayExists(x -> x = CAST('V0' AS Nullable(String)), v)) FROM t_str WHERE has(v, CAST('V0' AS Nullable(String)));
SELECT 'Nullable(FixedString) NULL needle',              groupArray(id) FROM t_str WHERE has(v, CAST(NULL AS Nullable(FixedString(3))));

-- Peeling the wrapper is only about a string-family layout. A nullable needle of any other type must
-- keep reaching the supertype cast: comparing it raw would equate a negative signed value with its
-- unsigned bit-pattern twin, and where the two types have no common type at all the refusal must
-- survive rather than turn into an answer.
SELECT '-- a nullable non-string needle keeps the supertype comparison';
DROP TABLE IF EXISTS t_i32;
DROP TABLE IF EXISTS t_u32;
DROP TABLE IF EXISTS t_i64_wide;
DROP TABLE IF EXISTS t_u64_wide;
DROP TABLE IF EXISTS t_i64_null;
DROP TABLE IF EXISTS t_date_n;
CREATE TABLE t_i32       (v Array(Int32))            ENGINE = Memory;
CREATE TABLE t_u32       (v Array(UInt32))           ENGINE = Memory;
CREATE TABLE t_i64_wide  (v Array(Int64))            ENGINE = Memory;
CREATE TABLE t_u64_wide  (v Array(UInt64))           ENGINE = Memory;
CREATE TABLE t_i64_null  (v Array(Nullable(Int64)))  ENGINE = Memory;
CREATE TABLE t_date_n    (v Array(Date))             ENGINE = Memory;
INSERT INTO t_i32      VALUES ([1, -1]);
INSERT INTO t_u32      VALUES ([4294967295, 1]);
INSERT INTO t_i64_wide VALUES ([1, -1]);
INSERT INTO t_u64_wide VALUES ([18446744073709551615, 1]);
INSERT INTO t_i64_null VALUES ([1, -1]);
INSERT INTO t_date_n   VALUES (['2020-01-01', '2021-01-01']);
SELECT 'Nullable(UInt32) needle on Int32', has(v, CAST(4294967295 AS Nullable(UInt32))), indexOf(v, CAST(4294967295 AS Nullable(UInt32))), countEqual(v, CAST(4294967295 AS Nullable(UInt32))), arrayExists(x -> assumeNotNull(x = CAST(4294967295 AS Nullable(UInt32))), v) FROM t_i32;
SELECT 'Nullable(Int32) needle on UInt32', has(v, CAST(-1 AS Nullable(Int32))), indexOf(v, CAST(-1 AS Nullable(Int32))), countEqual(v, CAST(-1 AS Nullable(Int32))), arrayExists(x -> assumeNotNull(x = CAST(-1 AS Nullable(Int32))), v) FROM t_u32;
-- At 64 bits there is no common type, so the refusal itself is the contract. The oracle above each
-- throw records what `=` answers, so a future change that starts answering can be judged against it.
SELECT 'Int64 element, Nullable(UInt64) needle oracle', arrayExists(x -> assumeNotNull(x = CAST(18446744073709551615 AS Nullable(UInt64))), v) FROM t_i64_wide;
SELECT has(v, CAST(18446744073709551615 AS Nullable(UInt64))) FROM t_i64_wide; -- { serverError NO_COMMON_TYPE }
SELECT 'UInt64 element, Nullable(Int64) needle oracle', arrayExists(x -> assumeNotNull(x = CAST(-1 AS Nullable(Int64))), v) FROM t_u64_wide;
SELECT has(v, CAST(-1 AS Nullable(Int64))) FROM t_u64_wide; -- { serverError NO_COMMON_TYPE }
-- The both-nullable path reaches the same peel, so it needs its own cell.
SELECT 'Array(Nullable(Int64)), Nullable(UInt64) needle oracle', arrayExists(x -> assumeNotNull(x = CAST(18446744073709551615 AS Nullable(UInt64))), v) FROM t_i64_null;
SELECT has(v, CAST(18446744073709551615 AS Nullable(UInt64))) FROM t_i64_null; -- { serverError NO_COMMON_TYPE }
-- Control: identical on both arms. A representable needle that IS present still matches.
SELECT 'Nullable(Int32) needle present', has(v, CAST(1 AS Nullable(Int32))), indexOf(v, CAST(1 AS Nullable(Int32))), arrayExists(x -> assumeNotNull(x = CAST(1 AS Nullable(Int32))), v) FROM t_i32;
-- Control: identical on both arms. A non-string, non-numeric needle the gate excludes.
SELECT 'Nullable(Date) needle', has(v, CAST('2020-01-01' AS Nullable(Date))), arrayExists(x -> assumeNotNull(x = CAST('2020-01-01' AS Nullable(Date))), v) FROM t_date_n;
DROP TABLE t_i32;
DROP TABLE t_u32;
DROP TABLE t_i64_wide;
DROP TABLE t_u64_wide;
DROP TABLE t_i64_null;
DROP TABLE t_date_n;

SELECT '-- constant array, compared as Fields';
SELECT has(['V0', 'V0\0'], toFixedString('V0', 3)), indexOf(['V0', 'V0\0'], toFixedString('V0', 3)), countEqual(['V0', 'V0\0'], toFixedString('V0', 3));
SELECT has(['V0', 'V0\0'], toFixedString('V0', 5)), has([toFixedString('V0', 4)], 'V0'), has([toFixedString('V0', 3)], 'V0\0\0\0');
SELECT 'per row', has(['V0', 'V0\0'], toFixedString('V0', 3)), indexOf(['V0', 'V0\0'], toFixedString('V0', 3)) FROM t_str ORDER BY id;
-- A non-const needle takes the per-row comparison in the constant-array handler, which the rows
-- above cannot reach: with a constant needle the whole array is scanned once instead.
SELECT 'non-const needle', has(['V0', 'V0\0'], materialize(toFixedString('V0', 3))), indexOf(['V0', 'V0\0'], materialize(toFixedString('V0', 3))), countEqual(['V0', 'V0\0'], materialize(toFixedString('V0', 3))), arrayFirstIndex(y -> y = materialize(toFixedString('V0', 3)), ['V0', 'V0\0']), arraySum(arrayMap(y -> toUInt8(y = materialize(toFixedString('V0', 3))), ['V0', 'V0\0']));
-- `indexOfAssumeSorted` assumes ascending order, and ['V0', 'V0\0'] is sorted bytewise, so its
-- precondition holds. Order under zero-padded comparison need not match the order the binary search
-- relies on, so a constant FixedString needle takes the linear scan instead. `arrayFirstIndex` is a
-- separate implementation, so it is an independent position oracle, and `indexOf` must agree too.
SELECT 'indexOfAssumeSorted',              indexOfAssumeSorted(['V0', 'V0\0'], toFixedString('V0', 3)), indexOf(['V0', 'V0\0'], toFixedString('V0', 3)), arrayFirstIndex(x -> x = toFixedString('V0', 3), ['V0', 'V0\0']);
SELECT 'indexOfAssumeSorted wider needle', indexOfAssumeSorted(['V0', 'V0\0'], toFixedString('V0', 5)), indexOf(['V0', 'V0\0'], toFixedString('V0', 5)), arrayFirstIndex(x -> x = toFixedString('V0', 5), ['V0', 'V0\0']);
-- Must not regress: a String needle keeps the binary search, which must stay correct.
SELECT 'indexOfAssumeSorted String needle', indexOfAssumeSorted(['V0', 'V0\0'], 'V0'), indexOfAssumeSorted(['V0', 'V0\0'], 'V0\0'), arrayFirstIndex(x -> x = 'V0', ['V0', 'V0\0']), arrayFirstIndex(x -> x = 'V0\0', ['V0', 'V0\0']);

SELECT '-- Map keys take the same path';
CREATE TABLE t_map    (id UInt64, m Map(String, UInt8)) ENGINE = Memory;
CREATE TABLE t_map_lc (id UInt64, m Map(LowCardinality(String), UInt8)) ENGINE = Memory;
INSERT INTO t_map    VALUES (0, {'V0':1}), (1, {'V0\0':1}), (2, {'X':1});
INSERT INTO t_map_lc VALUES (0, {'V0':1}), (1, {'V0\0':1}), (2, {'X':1});
SELECT 'Map',                 groupArray(id) FROM t_map    WHERE has(m, toFixedString('V0', 3));
SELECT 'Map(LowCardinality)', groupArray(id) FROM t_map_lc WHERE has(m, toFixedString('V0', 3));
-- `has(Map, ...)` strips LowCardinality before dispatch, so it cannot reach the LowCardinality
-- shortcut. `mapContainsValue` goes through the Map-to-array adapter, which keeps LowCardinality.
-- `mapContainsKey` reaches that adapter only with the rewrite disabled: at the default,
-- `FunctionToSubcolumnsPass` replaces it with `has(m.keys, ...)`, and there is no such rewrite for
-- `mapContainsValue`. Hence four key rows: both spellings x both element types.
CREATE TABLE t_map_val    (id UInt64, m Map(UInt8, String)) ENGINE = Memory;
CREATE TABLE t_map_val_lc (id UInt64, m Map(UInt8, LowCardinality(String))) ENGINE = Memory;
INSERT INTO t_map_val    VALUES (0, {0:'V0'}), (1, {1:'V0\0'}), (2, {2:'X'});
INSERT INTO t_map_val_lc VALUES (0, {0:'V0'}), (1, {1:'V0\0'}), (2, {2:'X'});
-- The rewrite is pinned on rather than left at the default: the test runner randomizes
-- `optimize_functions_to_subcolumns`, so without the pin these two rows take the same adapter path
-- as the `no-subcolumns` rows below on roughly half of all runs and the four rows collapse to two.
SELECT 'mapContainsKey',                   groupArray(id), (SELECT groupArray(id) FROM t_map        WHERE arrayExists(x -> x = toFixedString('V0', 3), mapKeys(m)))   FROM t_map        WHERE mapContainsKey(m, toFixedString('V0', 3)) SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'mapContainsKey LowCardinality',    groupArray(id), (SELECT groupArray(id) FROM t_map_lc     WHERE arrayExists(x -> x = toFixedString('V0', 3), mapKeys(m)))   FROM t_map_lc     WHERE mapContainsKey(m, toFixedString('V0', 3)) SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'mapContainsKey no-subcolumns',                groupArray(id), (SELECT groupArray(id) FROM t_map    WHERE arrayExists(x -> x = toFixedString('V0', 3), mapKeys(m)))   FROM t_map    WHERE mapContainsKey(m, toFixedString('V0', 3)) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'mapContainsKey LowCardinality no-subcolumns', groupArray(id), (SELECT groupArray(id) FROM t_map_lc WHERE arrayExists(x -> x = toFixedString('V0', 3), mapKeys(m)))   FROM t_map_lc WHERE mapContainsKey(m, toFixedString('V0', 3)) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'mapContainsValue',                 groupArray(id), (SELECT groupArray(id) FROM t_map_val    WHERE arrayExists(x -> x = toFixedString('V0', 3), mapValues(m))) FROM t_map_val    WHERE mapContainsValue(m, toFixedString('V0', 3));
SELECT 'mapContainsValue LowCardinality',  groupArray(id), (SELECT groupArray(id) FROM t_map_val_lc WHERE arrayExists(x -> x = toFixedString('V0', 3), mapValues(m))) FROM t_map_val_lc WHERE mapContainsValue(m, toFixedString('V0', 3));
-- The four key rows above only assert two distinct paths if the rewrite really is on at 1 and off at
-- 0, which is what these rows measure: 1 means the plan holds `has` over the `m.keys` subcolumn, 0
-- means it still holds `mapContainsKey` over the Map column. `enable_analyzer` is pinned on the OUTER
-- query because `EXPLAIN QUERY TREE` needs the analyzer, and a subquery may not change that setting.
SELECT 'rewrite fires at 1',                  countIf(explain LIKE '%function_name: has%'), countIf(explain LIKE '%column_name: m.keys%'), countIf(explain LIKE '%function_name: mapContainsKey%') FROM (EXPLAIN QUERY TREE SELECT mapContainsKey(m, toFixedString('V0', 3)) FROM t_map    SETTINGS optimize_functions_to_subcolumns = 1) SETTINGS enable_analyzer = 1;
SELECT 'rewrite declines at 0',               countIf(explain LIKE '%function_name: has%'), countIf(explain LIKE '%column_name: m.keys%'), countIf(explain LIKE '%function_name: mapContainsKey%') FROM (EXPLAIN QUERY TREE SELECT mapContainsKey(m, toFixedString('V0', 3)) FROM t_map    SETTINGS optimize_functions_to_subcolumns = 0) SETTINGS enable_analyzer = 1;
SELECT 'rewrite fires at 1 LowCardinality',   countIf(explain LIKE '%function_name: has%'), countIf(explain LIKE '%column_name: m.keys%'), countIf(explain LIKE '%function_name: mapContainsKey%') FROM (EXPLAIN QUERY TREE SELECT mapContainsKey(m, toFixedString('V0', 3)) FROM t_map_lc SETTINGS optimize_functions_to_subcolumns = 1) SETTINGS enable_analyzer = 1;
SELECT 'rewrite declines at 0 LowCardinality', countIf(explain LIKE '%function_name: has%'), countIf(explain LIKE '%column_name: m.keys%'), countIf(explain LIKE '%function_name: mapContainsKey%') FROM (EXPLAIN QUERY TREE SELECT mapContainsKey(m, toFixedString('V0', 3)) FROM t_map_lc SETTINGS optimize_functions_to_subcolumns = 0) SETTINGS enable_analyzer = 1;

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
-- Control, not coverage of the fix: this is the one wrapper and needle combination that both takes
-- the LowCardinality dictionary shortcut and passes through the Nullable peel. Measured identical on
-- master, because the shortcut casts the Nullable(FixedString(3)) needle down losslessly anyway.
SELECT 'nullable needle on LowCardinality(FixedString(3))', groupArray(id), (SELECT groupArray(id) FROM t_lc_fs3 WHERE arrayExists(x -> x = CAST(toFixedString('V0', 3) AS Nullable(FixedString(3))), v)) FROM t_lc_fs3 WHERE has(v, CAST(toFixedString('V0', 3) AS Nullable(FixedString(3))));
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
