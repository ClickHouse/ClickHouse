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

-- A nullable needle of a NON-string type must keep reaching the supertype cast: raw comparison would
-- equate a negative signed value with its unsigned twin, and where the two types have no common type
-- the refusal must survive rather than turn into an answer.
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
-- `indexOfAssumeSorted`'s ascending-order precondition holds (['V0', 'V0\0'] is sorted bytewise), but
-- zero-padded order need not match it, so a constant FixedString needle takes the linear scan.
-- `arrayFirstIndex` is a separate implementation, hence an independent position oracle.
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
-- `mapContainsKey` reaches the Map-to-array adapter (which keeps LowCardinality) ONLY with the rewrite
-- disabled: at the default, `FunctionToSubcolumnsPass` replaces it with `has(m.keys, ...)`, which strips
-- the wrapper. Hence the `optimize_functions_to_subcolumns = 0` pin, and four rows: both spellings x both types.
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
-- The four key rows above only assert two distinct paths if the rewrite really is on at 1 and off at 0,
-- which is what these rows measure. `enable_analyzer` is pinned on the OUTER query because
-- `EXPLAIN QUERY TREE` needs the analyzer and a subquery may not change that setting.
SELECT 'rewrite fires at 1',                  countIf(explain LIKE '%function_name: has%'), countIf(explain LIKE '%column_name: m.keys%'), countIf(explain LIKE '%function_name: mapContainsKey%') FROM (EXPLAIN QUERY TREE SELECT mapContainsKey(m, toFixedString('V0', 3)) FROM t_map    SETTINGS optimize_functions_to_subcolumns = 1) SETTINGS enable_analyzer = 1;
SELECT 'rewrite declines at 0',               countIf(explain LIKE '%function_name: has%'), countIf(explain LIKE '%column_name: m.keys%'), countIf(explain LIKE '%function_name: mapContainsKey%') FROM (EXPLAIN QUERY TREE SELECT mapContainsKey(m, toFixedString('V0', 3)) FROM t_map    SETTINGS optimize_functions_to_subcolumns = 0) SETTINGS enable_analyzer = 1;
SELECT 'rewrite fires at 1 LowCardinality',   countIf(explain LIKE '%function_name: has%'), countIf(explain LIKE '%column_name: m.keys%'), countIf(explain LIKE '%function_name: mapContainsKey%') FROM (EXPLAIN QUERY TREE SELECT mapContainsKey(m, toFixedString('V0', 3)) FROM t_map_lc SETTINGS optimize_functions_to_subcolumns = 1) SETTINGS enable_analyzer = 1;
SELECT 'rewrite declines at 0 LowCardinality', countIf(explain LIKE '%function_name: has%'), countIf(explain LIKE '%column_name: m.keys%'), countIf(explain LIKE '%function_name: mapContainsKey%') FROM (EXPLAIN QUERY TREE SELECT mapContainsKey(m, toFixedString('V0', 3)) FROM t_map_lc SETTINGS optimize_functions_to_subcolumns = 0) SETTINGS enable_analyzer = 1;

SELECT '-- the rewrite of arrayExists to has must preserve results';
SELECT 'FixedString needle', (SELECT groupArray(id) FROM t_str WHERE arrayExists(x -> x = toFixedString('V0', 3), v) SETTINGS optimize_rewrite_array_exists_to_has = 1)
                          = (SELECT groupArray(id) FROM t_str WHERE arrayExists(x -> x = toFixedString('V0', 3), v) SETTINGS optimize_rewrite_array_exists_to_has = 0);
SELECT 'String needle',      (SELECT groupArray(id) FROM t_str WHERE arrayExists(x -> x = 'V0', v) SETTINGS optimize_rewrite_array_exists_to_has = 1)
                          = (SELECT groupArray(id) FROM t_str WHERE arrayExists(x -> x = 'V0', v) SETTINGS optimize_rewrite_array_exists_to_has = 0);
-- The two rows above only compare two DIFFERENT plans where the rewrite fires at 1, and it declines
-- for a mismatched string-family pair, so the FixedString row compares one plan against itself and
-- the String row is the one that compares two. The rows below pin which is which; without them a
-- declined rewrite would read 1 for the wrong reason.
SELECT 'rewrite declines for a FixedString needle', countIf(explain LIKE '%function_name: has%'), countIf(explain LIKE '%function_name: arrayExists%') FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = toFixedString('V0', 3), v) FROM t_str SETTINGS optimize_rewrite_array_exists_to_has = 1) SETTINGS enable_analyzer = 1;
SELECT 'rewrite declines at 0',                     countIf(explain LIKE '%function_name: has%'), countIf(explain LIKE '%function_name: arrayExists%') FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = toFixedString('V0', 3), v) FROM t_str SETTINGS optimize_rewrite_array_exists_to_has = 0) SETTINGS enable_analyzer = 1;
SELECT 'rewrite fires for a String needle',         countIf(explain LIKE '%function_name: has%'), countIf(explain LIKE '%function_name: arrayExists%') FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = 'V0', v)                   FROM t_str SETTINGS optimize_rewrite_array_exists_to_has = 1) SETTINGS enable_analyzer = 1;
SELECT 'String needle declines at 0',                countIf(explain LIKE '%function_name: has%'), countIf(explain LIKE '%function_name: arrayExists%') FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = 'V0', v)                   FROM t_str SETTINGS optimize_rewrite_array_exists_to_has = 0) SETTINGS enable_analyzer = 1;

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
-- A plain `String` needle against `LowCardinality(FixedString(N))`: padded up to N by the lookup's own
-- cast, exact while it FITS, so at most N bytes still denotes one value. A longer one must decline.
DROP TABLE IF EXISTS t_fs3_wide;
CREATE TABLE t_fs3_wide (id UInt64, v Array(FixedString(3))) ENGINE = Memory;
INSERT INTO t_fs3_wide VALUES (0, ['V0']), (1, ['X']);
SELECT 'String needle shorter than the element',     groupArray(id), (SELECT groupArray(id) FROM t_lc_fs3 WHERE arrayExists(x -> x = 'V0', v)      SETTINGS optimize_rewrite_array_exists_to_has = 0) FROM t_lc_fs3 WHERE has(v, 'V0');
SELECT 'String needle exactly the element width',    groupArray(id), (SELECT groupArray(id) FROM t_lc_fs3 WHERE arrayExists(x -> x = 'V0\0', v)    SETTINGS optimize_rewrite_array_exists_to_has = 0) FROM t_lc_fs3 WHERE has(v, 'V0\0');
-- The pinned answer deliberately DISAGREES with an `=` oracle: `=` pads a plain `String` needle while
-- every membership path truncates it to the element width. That is a separate pre-existing defect,
-- measured identical on master for a non-LowCardinality element, whose answer is the control beside it.
SELECT 'String needle wider than the element',       groupArray(id), (SELECT groupArray(id) FROM t_fs3_wide WHERE has(v, 'V0\0\0')) FROM t_lc_fs3 WHERE has(v, 'V0\0\0');
SELECT 'String needle indexOf exactly the width',    groupArray(id), (SELECT groupArray(id) FROM t_lc_fs3 WHERE arrayExists(x -> x = 'V0\0', v)    SETTINGS optimize_rewrite_array_exists_to_has = 0) FROM t_lc_fs3 WHERE indexOf(v, 'V0\0') > 0;
SELECT 'String needle countEqual exactly the width', groupArray(id), (SELECT groupArray(id) FROM t_lc_fs3 WHERE arrayExists(x -> x = 'V0\0', v)    SETTINGS optimize_rewrite_array_exists_to_has = 0) FROM t_lc_fs3 WHERE countEqual(v, 'V0\0') > 0;
-- The Map adapter keeps the wrapper, so a Map value reaches the same guard with the same needle.
DROP TABLE IF EXISTS t_map_val_lc_fs3;
CREATE TABLE t_map_val_lc_fs3 (id UInt64, m Map(UInt8, LowCardinality(FixedString(3)))) ENGINE = Memory;
INSERT INTO t_map_val_lc_fs3 VALUES (0, {0:'V0'}), (1, {1:'X'});
SELECT 'String needle mapContainsValue exactly the width', groupArray(id), (SELECT groupArray(id) FROM t_map_val_lc_fs3 WHERE arrayExists(x -> x = 'V0\0', mapValues(m)) SETTINGS optimize_rewrite_array_exists_to_has = 0) FROM t_map_val_lc_fs3 WHERE mapContainsValue(m, 'V0\0');
DROP TABLE t_map_val_lc_fs3;
DROP TABLE t_fs3_wide;
-- CONTROL, not coverage: this combination is answered by the dictionary shortcut, which returns before
-- `executeArrayImpl` is entered and so BYPASSES the Nullable peel. Measured identical on master.
SELECT 'nullable needle on LowCardinality(FixedString(3))', groupArray(id), (SELECT groupArray(id) FROM t_lc_fs3 WHERE arrayExists(x -> x = CAST(toFixedString('V0', 3) AS Nullable(FixedString(3))), v)) FROM t_lc_fs3 WHERE has(v, CAST(toFixedString('V0', 3) AS Nullable(FixedString(3))));
-- The NULL payload of the same shape. The dictionary shortcut resolves a NULL needle to index 0,
-- which is the NULL slot only for a nullable dictionary; on a non-nullable one index 0 is the type's
-- default value, so the needle used to match every row holding that default.
DROP TABLE IF EXISTS t_lc_fs3_def;
DROP TABLE IF EXISTS t_fs3_def;
DROP TABLE IF EXISTS t_lc_s_def;
DROP TABLE IF EXISTS t_s_def;
CREATE TABLE t_lc_fs3_def (id UInt64, v Array(LowCardinality(FixedString(3)))) ENGINE = Memory;
CREATE TABLE t_fs3_def    (id UInt64, v Array(FixedString(3)))                 ENGINE = Memory;
CREATE TABLE t_lc_s_def   (id UInt64, v Array(LowCardinality(String)))         ENGINE = Memory;
CREATE TABLE t_s_def      (id UInt64, v Array(String))                         ENGINE = Memory;
INSERT INTO t_lc_fs3_def VALUES (0, [toFixedString('', 3)]), (1, ['V0']);
INSERT INTO t_fs3_def    VALUES (0, [toFixedString('', 3)]), (1, ['V0']);
INSERT INTO t_lc_s_def   VALUES (0, ['']), (1, ['V0']);
INSERT INTO t_s_def      VALUES (0, ['']), (1, ['V0']);
SELECT 'NULL needle on LowCardinality(FixedString(3))', groupArray(id), (SELECT groupArray(id) FROM t_lc_fs3_def WHERE arrayExists(x -> assumeNotNull(x = CAST(NULL AS Nullable(FixedString(3)))), v)) FROM t_lc_fs3_def WHERE has(v, CAST(NULL AS Nullable(FixedString(3))));
-- Control, not coverage: a non-LowCardinality element type never enters the shortcut at all.
SELECT 'NULL needle on FixedString(3)',                groupArray(id) FROM t_fs3_def   WHERE has(v, CAST(NULL AS Nullable(FixedString(3))));
SELECT 'NULL needle on LowCardinality(String)',        groupArray(id), (SELECT groupArray(id) FROM t_lc_s_def   WHERE arrayExists(x -> assumeNotNull(x = CAST(NULL AS Nullable(String))), v))            FROM t_lc_s_def   WHERE has(v, CAST(NULL AS Nullable(String)));
-- Control, not coverage: same reason as the FixedString(3) control above.
SELECT 'NULL needle on String',                       groupArray(id) FROM t_s_def      WHERE has(v, CAST(NULL AS Nullable(String)));
-- The shortcut is shared by every action except `indexOfAssumeSorted`, so a boolean is the weakest
-- of the three answers it can give: a position and a count over the default-valued rows say more.
SELECT 'NULL needle indexOf on LowCardinality(FixedString(3))',    groupArray(id), (SELECT groupArray(id) FROM t_lc_fs3_def WHERE arrayExists(x -> assumeNotNull(x = CAST(NULL AS Nullable(FixedString(3)))), v)) FROM t_lc_fs3_def WHERE indexOf(v, CAST(NULL AS Nullable(FixedString(3)))) > 0;
SELECT 'NULL needle countEqual on LowCardinality(FixedString(3))', groupArray(id), (SELECT groupArray(id) FROM t_lc_fs3_def WHERE arrayExists(x -> assumeNotNull(x = CAST(NULL AS Nullable(FixedString(3)))), v)) FROM t_lc_fs3_def WHERE countEqual(v, CAST(NULL AS Nullable(FixedString(3)))) > 0;
SELECT 'NULL needle indexOf on LowCardinality(String)',            groupArray(id), (SELECT groupArray(id) FROM t_lc_s_def   WHERE arrayExists(x -> assumeNotNull(x = CAST(NULL AS Nullable(String))), v))            FROM t_lc_s_def   WHERE indexOf(v, CAST(NULL AS Nullable(String))) > 0;
SELECT 'NULL needle countEqual on LowCardinality(String)',         groupArray(id), (SELECT groupArray(id) FROM t_lc_s_def   WHERE arrayExists(x -> assumeNotNull(x = CAST(NULL AS Nullable(String))), v))            FROM t_lc_s_def   WHERE countEqual(v, CAST(NULL AS Nullable(String))) > 0;
-- The Map entry points reach the same shortcut through the Map-to-array adapter, which builds
-- `Array(LowCardinality(String))` and so keeps the wrapper. `mapContainsKey` needs the subcolumn
-- rewrite off to get there, exactly as the key rows above.
DROP TABLE IF EXISTS t_map_val_lc_def;
DROP TABLE IF EXISTS t_map_lc_def;
CREATE TABLE t_map_val_lc_def (id UInt64, m Map(UInt8, LowCardinality(String))) ENGINE = Memory;
CREATE TABLE t_map_lc_def     (id UInt64, m Map(LowCardinality(String), UInt8)) ENGINE = Memory;
INSERT INTO t_map_val_lc_def VALUES (0, {0:''}), (1, {1:'V0'});
INSERT INTO t_map_lc_def     VALUES (0, {'':1}), (1, {'V0':1});
SELECT 'NULL needle mapContainsValue LowCardinality',              groupArray(id), (SELECT groupArray(id) FROM t_map_val_lc_def WHERE arrayExists(x -> assumeNotNull(x = CAST(NULL AS Nullable(String))), mapValues(m))) FROM t_map_val_lc_def WHERE mapContainsValue(m, CAST(NULL AS Nullable(String)));
SELECT 'NULL needle mapContainsKey LowCardinality no-subcolumns',  groupArray(id), (SELECT groupArray(id) FROM t_map_lc_def     WHERE arrayExists(x -> assumeNotNull(x = CAST(NULL AS Nullable(String))), mapKeys(m)))   FROM t_map_lc_def     WHERE mapContainsKey(m, CAST(NULL AS Nullable(String))) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'longer non-NUL String needle',      has(v, materialize('V0abc')) FROM t_fs4 WHERE id = 0;
-- Control for the ALLOW-LIST, not for the guard: a bare NULL is typed Nullable(Nothing), and
-- `Nothing` is not the element type, so the shortcut is refused one line later by
-- needleMapsToSingleDictionaryValue and never reaches the nullability test.
SELECT 'NULL needle',                       groupArray(id) FROM t_lc_null WHERE has(v, NULL);
-- Must-not-regress: a typed NULL needle on a NULLABLE dictionary must keep finding the NULL element and
-- must not match the default-valued row. Asserts that RESULT only; which implementation answers is not
-- observable from SQL. The oracle is `isNull` because `equals(NULL, NULL)` is NULL, so `= NULL` answers [].
DROP TABLE IF EXISTS t_lc_null_def;
DROP TABLE IF EXISTS t_lc_nfs3_def;
CREATE TABLE t_lc_null_def  (id UInt64, v Array(LowCardinality(Nullable(String))))         ENGINE = Memory;
CREATE TABLE t_lc_nfs3_def  (id UInt64, v Array(LowCardinality(Nullable(FixedString(3))))) ENGINE = Memory;
INSERT INTO t_lc_null_def  VALUES (0, [NULL]), (1, ['']), (2, ['V0']);
INSERT INTO t_lc_nfs3_def  VALUES (0, [NULL]), (1, [toFixedString('', 3)]), (2, ['V0']);
SELECT 'typed NULL needle on LowCardinality(Nullable(String))',            groupArray(id), (SELECT groupArray(id) FROM t_lc_null_def WHERE arrayExists(x -> isNull(x), v)) FROM t_lc_null_def WHERE has(v, CAST(NULL AS Nullable(String)));
SELECT 'typed NULL needle on LowCardinality(Nullable(FixedString(3)))',    groupArray(id), (SELECT groupArray(id) FROM t_lc_nfs3_def WHERE arrayExists(x -> isNull(x), v)) FROM t_lc_nfs3_def WHERE has(v, CAST(NULL AS Nullable(FixedString(3))));
SELECT 'typed NULL needle position on LowCardinality(Nullable(String))',   groupArray(id), (SELECT groupArray(id) FROM t_lc_null_def WHERE arrayExists(x -> isNull(x), v)) FROM t_lc_null_def WHERE indexOf(v, CAST(NULL AS Nullable(String))) > 0;

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

-- Every unsigned width against its own `=` oracle, so the widest element is evidenced rather than
-- inferred. The plain array is shown for the maximum needle ONLY: for `-1` at 32 and 64 bits it answers
-- [0] against an oracle of [], a pre-existing defect of the non-LowCardinality path (see `Int64` below).
DROP TABLE IF EXISTS t_u16_lc;
DROP TABLE IF EXISTS t_u16;
DROP TABLE IF EXISTS t_u32_lc;
DROP TABLE IF EXISTS t_u32;
DROP TABLE IF EXISTS t_u64_lc;
DROP TABLE IF EXISTS t_u64;
CREATE TABLE t_u16_lc (id UInt64, v Array(LowCardinality(UInt16))) ENGINE = Memory;
CREATE TABLE t_u16    (id UInt64, v Array(UInt16))                ENGINE = Memory;
CREATE TABLE t_u32_lc (id UInt64, v Array(LowCardinality(UInt32))) ENGINE = Memory;
CREATE TABLE t_u32    (id UInt64, v Array(UInt32))                ENGINE = Memory;
CREATE TABLE t_u64_lc (id UInt64, v Array(LowCardinality(UInt64))) ENGINE = Memory;
CREATE TABLE t_u64    (id UInt64, v Array(UInt64))                ENGINE = Memory;
INSERT INTO t_u16_lc VALUES (0, [0, 65535, 65534]);
INSERT INTO t_u16    VALUES (0, [0, 65535, 65534]);
INSERT INTO t_u32_lc VALUES (0, [0, 4294967295, 4294967294]);
INSERT INTO t_u32    VALUES (0, [0, 4294967295, 4294967294]);
INSERT INTO t_u64_lc VALUES (0, [0, 18446744073709551615, 18446744073709551614]);
INSERT INTO t_u64    VALUES (0, [0, 18446744073709551615, 18446744073709551614]);
SELECT 'has -1 on UInt16',  groupArray(id), (SELECT groupArray(id) FROM t_u16 WHERE arrayExists(x -> x = -1, v)) FROM t_u16_lc WHERE has(v, -1);
SELECT 'has -1 on UInt32',  groupArray(id), (SELECT groupArray(id) FROM t_u32 WHERE arrayExists(x -> x = -1, v)) FROM t_u32_lc WHERE has(v, -1);
SELECT 'has -1 on UInt64',  groupArray(id), (SELECT groupArray(id) FROM t_u64 WHERE arrayExists(x -> x = -1, v)) FROM t_u64_lc WHERE has(v, -1);
SELECT 'has max on UInt16', groupArray(id), (SELECT groupArray(id) FROM t_u16 WHERE arrayExists(x -> x = 65535, v)),               (SELECT groupArray(id) FROM t_u16 WHERE has(v, 65535)) FROM t_u16_lc WHERE has(v, 65535);
SELECT 'has max on UInt32', groupArray(id), (SELECT groupArray(id) FROM t_u32 WHERE arrayExists(x -> x = 4294967295, v)),          (SELECT groupArray(id) FROM t_u32 WHERE has(v, 4294967295)) FROM t_u32_lc WHERE has(v, 4294967295);
SELECT 'has max on UInt64', groupArray(id), (SELECT groupArray(id) FROM t_u64 WHERE arrayExists(x -> x = 18446744073709551615, v)), (SELECT groupArray(id) FROM t_u64 WHERE has(v, 18446744073709551615)) FROM t_u64_lc WHERE has(v, 18446744073709551615);
DROP TABLE t_u16_lc;
DROP TABLE t_u16;
DROP TABLE t_u32_lc;
DROP TABLE t_u32;
DROP TABLE t_u64_lc;
DROP TABLE t_u64;

-- A needle of a DIFFERENT type is admitted exactly when converting it into the element type preserves
-- its value. Both directions matter: one that converts exactly must keep the shortcut, one that does not
-- must decline.
DROP TABLE IF EXISTS t_i64_exact_lc;
DROP TABLE IF EXISTS t_i64_exact;
CREATE TABLE t_i64_exact_lc (id UInt64, v Array(LowCardinality(Int64))) ENGINE = Memory;
CREATE TABLE t_i64_exact    (id UInt64, v Array(Int64))                 ENGINE = Memory;
-- 9007199254740993 is the first integer a Float64 cannot hold: it rounds to ...992, which the array
-- does NOT contain, so the needle must not match. Row 1 holds ...992 itself, which it must match.
INSERT INTO t_i64_exact_lc VALUES (0, [9007199254740993]), (1, [9007199254740992]);
INSERT INTO t_i64_exact    VALUES (0, [9007199254740993]), (1, [9007199254740992]);
SELECT 'Float64 needle exactly representable', groupArray(id), (SELECT groupArray(id) FROM t_i64_exact WHERE arrayExists(x -> x = 9007199254740992.0, v) SETTINGS optimize_rewrite_array_exists_to_has = 0) FROM t_i64_exact_lc WHERE has(v, 9007199254740992.0);
SELECT 'Float64 needle indexOf',               groupArray(id), (SELECT groupArray(id) FROM t_i64_exact WHERE arrayExists(x -> x = 9007199254740992.0, v) SETTINGS optimize_rewrite_array_exists_to_has = 0) FROM t_i64_exact_lc WHERE indexOf(v, 9007199254740992.0) > 0;
SELECT 'Float64 needle countEqual',            groupArray(id), (SELECT groupArray(id) FROM t_i64_exact WHERE arrayExists(x -> x = 9007199254740992.0, v) SETTINGS optimize_rewrite_array_exists_to_has = 0) FROM t_i64_exact_lc WHERE countEqual(v, 9007199254740992.0) > 0;
DROP TABLE t_i64_exact_lc;
DROP TABLE t_i64_exact;

-- A temporal pair, where one cast cannot report the loss: `DateTime -> Date` truncates the time of day
-- and SUCCEEDS, so only converting back separates it from an exact midnight. The midnight needle must
-- match, the non-midnight one must not.
DROP TABLE IF EXISTS t_date_lc;
DROP TABLE IF EXISTS t_date;
CREATE TABLE t_date_lc (id UInt64, v Array(LowCardinality(Date))) ENGINE = Memory;
CREATE TABLE t_date    (id UInt64, v Array(Date))                 ENGINE = Memory;
INSERT INTO t_date_lc VALUES (0, [toDate('1970-01-02')]), (1, [toDate('1970-01-10')]);
INSERT INTO t_date    VALUES (0, [toDate('1970-01-02')]), (1, [toDate('1970-01-10')]);
SELECT 'DateTime needle at midnight',      groupArray(id), (SELECT groupArray(id) FROM t_date WHERE arrayExists(x -> x = toDateTime('1970-01-02 00:00:00', 'UTC'), v) SETTINGS optimize_rewrite_array_exists_to_has = 0) FROM t_date_lc WHERE has(v, toDateTime('1970-01-02 00:00:00', 'UTC'));
SELECT 'DateTime needle not at midnight',  groupArray(id), (SELECT groupArray(id) FROM t_date WHERE arrayExists(x -> x = toDateTime('1970-01-02 01:00:00', 'UTC'), v) SETTINGS optimize_rewrite_array_exists_to_has = 0) FROM t_date_lc WHERE has(v, toDateTime('1970-01-02 01:00:00', 'UTC'));
SELECT 'DateTime needle midnight indexOf', groupArray(id), (SELECT groupArray(id) FROM t_date WHERE arrayExists(x -> x = toDateTime('1970-01-02 00:00:00', 'UTC'), v) SETTINGS optimize_rewrite_array_exists_to_has = 0) FROM t_date_lc WHERE indexOf(v, toDateTime('1970-01-02 00:00:00', 'UTC')) > 0;
SELECT 'DateTime needle midnight count',   groupArray(id), (SELECT groupArray(id) FROM t_date WHERE arrayExists(x -> x = toDateTime('1970-01-02 00:00:00', 'UTC'), v) SETTINGS optimize_rewrite_array_exists_to_has = 0) FROM t_date_lc WHERE countEqual(v, toDateTime('1970-01-02 00:00:00', 'UTC')) > 0;
DROP TABLE t_date_lc;
DROP TABLE t_date;

-- A declined needle must not be answered by comparing RAW PHYSICAL NUMBERS: a `Date` day number and a
-- `DateTime` epoch second share a physical value. The rows above use 3600 seconds and never collide, so
-- they would pass for a reason unrelated to the guard; these use colliding values and keep those as control.
DROP TABLE IF EXISTS t_coll_date_lc;
DROP TABLE IF EXISTS t_coll_dt_lc;
CREATE TABLE t_coll_date_lc (id UInt64, v Array(LowCardinality(Date)))            ENGINE = Memory;
CREATE TABLE t_coll_dt_lc   (id UInt64, v Array(LowCardinality(DateTime('UTC')))) ENGINE = Memory;
INSERT INTO t_coll_date_lc VALUES (0, [toDate('1970-01-02')]);
INSERT INTO t_coll_dt_lc   VALUES (0, [toDateTime(1, 'UTC')]);
SELECT 'Date day 1 vs DateTime second 1',         groupArray(id), (SELECT groupArray(id) FROM t_coll_date_lc WHERE arrayExists(x -> x = toDateTime(1, 'UTC'), v)) FROM t_coll_date_lc WHERE has(v, toDateTime(1, 'UTC'));
SELECT 'Date day 1 vs DateTime second 1 indexOf', groupArray(id), (SELECT groupArray(id) FROM t_coll_date_lc WHERE arrayExists(x -> x = toDateTime(1, 'UTC'), v)) FROM t_coll_date_lc WHERE indexOf(v, toDateTime(1, 'UTC')) > 0;
SELECT 'Date day 1 vs DateTime second 1 count',   groupArray(id), (SELECT groupArray(id) FROM t_coll_date_lc WHERE arrayExists(x -> x = toDateTime(1, 'UTC'), v)) FROM t_coll_date_lc WHERE countEqual(v, toDateTime(1, 'UTC')) > 0;
SELECT 'DateTime second 1 vs Date day 1',         groupArray(id), (SELECT groupArray(id) FROM t_coll_dt_lc   WHERE arrayExists(x -> x = toDate('1970-01-02'), v)) FROM t_coll_dt_lc   WHERE has(v, toDate('1970-01-02'));
SELECT 'DateTime second 1 vs Date day 1 indexOf', groupArray(id), (SELECT groupArray(id) FROM t_coll_dt_lc   WHERE arrayExists(x -> x = toDate('1970-01-02'), v)) FROM t_coll_dt_lc   WHERE indexOf(v, toDate('1970-01-02')) > 0;
SELECT 'DateTime second 1 vs Date day 1 count',   groupArray(id), (SELECT groupArray(id) FROM t_coll_dt_lc   WHERE arrayExists(x -> x = toDate('1970-01-02'), v)) FROM t_coll_dt_lc   WHERE countEqual(v, toDate('1970-01-02')) > 0;
DROP TABLE t_coll_date_lc;
DROP TABLE t_coll_dt_lc;

-- The numeric member of the same class: 2^63 as a `Float64` is what the `Int64` maximum ROUNDS to, so a
-- raw comparison equates them while `=` does not. Only the LowCardinality row is asserted against its
-- oracle; the plain array answers [0] against [] on master too, a separate pre-existing defect.
DROP TABLE IF EXISTS t_coll_i64_lc;
CREATE TABLE t_coll_i64_lc (id UInt64, v Array(LowCardinality(Int64))) ENGINE = Memory;
INSERT INTO t_coll_i64_lc VALUES (0, [9223372036854775807]);
SELECT 'Int64 max vs Float64 2^63', groupArray(id), (SELECT groupArray(id) FROM t_coll_i64_lc WHERE arrayExists(x -> x = 9223372036854775808.0::Float64, v)) FROM t_coll_i64_lc WHERE has(v, 9223372036854775808.0::Float64);
DROP TABLE t_coll_i64_lc;

-- A `nan`/`inf` needle has no integral counterpart, so it matches nothing and must ANSWER that rather
-- than propagate the guard probe's own conversion failure. The plain array always answered 0; only the
-- `LowCardinality` spelling raised `Code 70`.
DROP TABLE IF EXISTS t_nan_lc;
DROP TABLE IF EXISTS t_nan;
CREATE TABLE t_nan_lc (id UInt64, v Array(LowCardinality(Int8))) ENGINE = Memory;
CREATE TABLE t_nan    (id UInt64, v Array(Int8))                 ENGINE = Memory;
INSERT INTO t_nan_lc VALUES (0, [1]);
INSERT INTO t_nan    VALUES (0, [1]);
SELECT 'nan needle',         groupArray(id), (SELECT groupArray(id) FROM t_nan_lc WHERE arrayExists(x -> x = nan, v)), (SELECT groupArray(id) FROM t_nan WHERE has(v, nan)) FROM t_nan_lc WHERE has(v, nan);
SELECT 'inf needle',         groupArray(id), (SELECT groupArray(id) FROM t_nan_lc WHERE arrayExists(x -> x = inf, v)), (SELECT groupArray(id) FROM t_nan WHERE has(v, inf)) FROM t_nan_lc WHERE has(v, inf);
SELECT 'nan needle indexOf', groupArray(id), (SELECT groupArray(id) FROM t_nan_lc WHERE arrayExists(x -> x = nan, v)) FROM t_nan_lc WHERE indexOf(v, nan) > 0;
SELECT 'nan needle count',   groupArray(id), (SELECT groupArray(id) FROM t_nan_lc WHERE arrayExists(x -> x = nan, v)) FROM t_nan_lc WHERE countEqual(v, nan) > 0;
DROP TABLE t_nan_lc;
DROP TABLE t_nan;

-- The OPPOSITE conversion failure: a type PAIR with no implementation, whose control is the `nan` rows
-- above (the value case must still zero-fill). `IPv4`/`UInt8` share a supertype, so a guard reading any
-- refusal as "no element can equal it" answers 0 where `=` answers 1.
DROP TABLE IF EXISTS t_ip_lc;
DROP TABLE IF EXISTS t_ip;
DROP TABLE IF EXISTS t_ipu32_lc;
DROP TABLE IF EXISTS t_ipu32;
CREATE TABLE t_ip_lc    (id UInt64, v Array(LowCardinality(IPv4)))   ENGINE = Memory;
CREATE TABLE t_ip       (id UInt64, v Array(IPv4))                   ENGINE = Memory;
CREATE TABLE t_ipu32_lc (id UInt64, v Array(LowCardinality(UInt32))) ENGINE = Memory;
CREATE TABLE t_ipu32    (id UInt64, v Array(UInt32))                 ENGINE = Memory;
INSERT INTO t_ip_lc    VALUES (0, [toIPv4('0.0.0.1')]);
INSERT INTO t_ip       VALUES (0, [toIPv4('0.0.0.1')]);
INSERT INTO t_ipu32_lc VALUES (0, [1]);
INSERT INTO t_ipu32    VALUES (0, [1]);
SELECT 'IPv4 element, UInt8 needle',         groupArray(id), (SELECT groupArray(id) FROM t_ip WHERE arrayExists(x -> x = 1::UInt8, v)), (SELECT groupArray(id) FROM t_ip WHERE has(v, 1::UInt8)) FROM t_ip_lc WHERE has(v, 1::UInt8);
SELECT 'IPv4 element, UInt8 needle indexOf', groupArray(id), (SELECT groupArray(id) FROM t_ip WHERE arrayExists(x -> x = 1::UInt8, v)) FROM t_ip_lc WHERE indexOf(v, 1::UInt8) > 0;
SELECT 'IPv4 element, UInt8 needle count',   groupArray(id), (SELECT groupArray(id) FROM t_ip WHERE arrayExists(x -> x = 1::UInt8, v)) FROM t_ip_lc WHERE countEqual(v, 1::UInt8) > 0;
-- The negative control in the same pair: the fix must restore the general comparison, not match always.
SELECT 'IPv4 element, non-matching needle',  groupArray(id), (SELECT groupArray(id) FROM t_ip WHERE arrayExists(x -> x = 2::UInt8, v)), (SELECT groupArray(id) FROM t_ip WHERE has(v, 2::UInt8)) FROM t_ip_lc WHERE has(v, 2::UInt8);
-- The mirrored direction fails the same way for a different reason: `UInt32 -> IPv4` has a plain cast
-- but no ACCURATE one, so the round trip's way back answers NULL rather than throwing. Both must be
-- read as facts about the pair.
SELECT 'UInt32 element, IPv4 needle',         groupArray(id), (SELECT groupArray(id) FROM t_ipu32 WHERE arrayExists(x -> x = toIPv4('0.0.0.1'), v)), (SELECT groupArray(id) FROM t_ipu32 WHERE has(v, toIPv4('0.0.0.1'))) FROM t_ipu32_lc WHERE has(v, toIPv4('0.0.0.1'));
SELECT 'UInt32 element, IPv4 needle indexOf', groupArray(id), (SELECT groupArray(id) FROM t_ipu32 WHERE arrayExists(x -> x = toIPv4('0.0.0.1'), v)) FROM t_ipu32_lc WHERE indexOf(v, toIPv4('0.0.0.1')) > 0;
SELECT 'UInt32 element, IPv4 needle count',   groupArray(id), (SELECT groupArray(id) FROM t_ipu32 WHERE arrayExists(x -> x = toIPv4('0.0.0.1'), v)) FROM t_ipu32_lc WHERE countEqual(v, toIPv4('0.0.0.1')) > 0;
SELECT 'UInt32 element, non-matching IPv4',   groupArray(id), (SELECT groupArray(id) FROM t_ipu32 WHERE arrayExists(x -> x = toIPv4('0.0.0.2'), v)), (SELECT groupArray(id) FROM t_ipu32 WHERE has(v, toIPv4('0.0.0.2'))) FROM t_ipu32_lc WHERE has(v, toIPv4('0.0.0.2'));
-- Both Map spellings of the same pair.
DROP TABLE IF EXISTS t_ip_map_val;
DROP TABLE IF EXISTS t_ip_map_key;
CREATE TABLE t_ip_map_val (id UInt64, m Map(UInt8, LowCardinality(IPv4))) ENGINE = Memory;
CREATE TABLE t_ip_map_key (id UInt64, m Map(LowCardinality(IPv4), UInt8)) ENGINE = Memory;
INSERT INTO t_ip_map_val VALUES (0, map(1, toIPv4('0.0.0.1')));
INSERT INTO t_ip_map_key VALUES (0, map(toIPv4('0.0.0.1'), 1));
SELECT 'mapContainsValue IPv4 element', groupArray(id), (SELECT groupArray(id) FROM t_ip_map_val WHERE arrayExists(x -> x = 1::UInt8, mapValues(m))) FROM t_ip_map_val WHERE mapContainsValue(m, 1::UInt8);
SELECT 'mapContainsKey IPv4 element',   groupArray(id), (SELECT groupArray(id) FROM t_ip_map_key WHERE arrayExists(x -> x = 1::UInt8, mapKeys(m))) FROM t_ip_map_key WHERE mapContainsKey(m, 1::UInt8) SETTINGS optimize_functions_to_subcolumns = 0;
DROP TABLE t_ip_map_val;
DROP TABLE t_ip_map_key;
DROP TABLE t_ip_lc;
DROP TABLE t_ip;
DROP TABLE t_ipu32_lc;
DROP TABLE t_ipu32;

SELECT '-- a LowCardinality wrapper on the NEEDLE must not change the answer';
-- The byte-length read must peel the same wrappers as the type side, or the very same three bytes are
-- admitted as `String` and declined as `LowCardinality(String)`. Both wrapper orders can arrive.
DROP TABLE IF EXISTS t_needle_lc;
CREATE TABLE t_needle_lc (id UInt64, v Array(LowCardinality(FixedString(3)))) ENGINE = Memory;
INSERT INTO t_needle_lc VALUES (0, ['V0']);
SELECT 'String exact width',               groupArray(id), (SELECT groupArray(id) FROM t_needle_lc WHERE arrayExists(x -> x = 'V0\0', v)) FROM t_needle_lc WHERE has(v, 'V0\0');
SELECT 'LowCardinality(String)',           groupArray(id), (SELECT groupArray(id) FROM t_needle_lc WHERE arrayExists(x -> x = toLowCardinality('V0\0'), v)) FROM t_needle_lc WHERE has(v, toLowCardinality('V0\0'));
SELECT 'LowCardinality(String) indexOf',   groupArray(id), (SELECT groupArray(id) FROM t_needle_lc WHERE arrayExists(x -> x = toLowCardinality('V0\0'), v)) FROM t_needle_lc WHERE indexOf(v, toLowCardinality('V0\0')) > 0;
SELECT 'LowCardinality(String) count',     groupArray(id), (SELECT groupArray(id) FROM t_needle_lc WHERE arrayExists(x -> x = toLowCardinality('V0\0'), v)) FROM t_needle_lc WHERE countEqual(v, toLowCardinality('V0\0')) > 0;
SELECT 'Nullable(String)',                 groupArray(id), (SELECT groupArray(id) FROM t_needle_lc WHERE arrayExists(x -> assumeNotNull(x = CAST('V0\0' AS Nullable(String))), v)) FROM t_needle_lc WHERE has(v, CAST('V0\0' AS Nullable(String)));
SELECT 'LowCardinality(Nullable(String))', groupArray(id), (SELECT groupArray(id) FROM t_needle_lc WHERE arrayExists(x -> assumeNotNull(x = toLowCardinality(CAST('V0\0' AS Nullable(String)))), v)) FROM t_needle_lc WHERE has(v, toLowCardinality(CAST('V0\0' AS Nullable(String))));
SELECT 'LowCardinality(String) shorter',   groupArray(id), (SELECT groupArray(id) FROM t_needle_lc WHERE arrayExists(x -> x = toLowCardinality('V0'), v)) FROM t_needle_lc WHERE has(v, toLowCardinality('V0'));
DROP TABLE t_needle_lc;

-- The same needle wrapper through both Map spellings.
DROP TABLE IF EXISTS t_needle_map_val;
DROP TABLE IF EXISTS t_needle_map_key;
CREATE TABLE t_needle_map_val (id UInt64, m Map(UInt8, LowCardinality(FixedString(3)))) ENGINE = Memory;
CREATE TABLE t_needle_map_key (id UInt64, m Map(LowCardinality(FixedString(3)), UInt8)) ENGINE = Memory;
INSERT INTO t_needle_map_val VALUES (0, map(1, 'V0'));
INSERT INTO t_needle_map_key VALUES (0, map('V0', 1));
SELECT 'mapContainsValue LowCardinality needle', groupArray(id), (SELECT groupArray(id) FROM t_needle_map_val WHERE arrayExists(x -> x = toLowCardinality('V0\0'), mapValues(m))) FROM t_needle_map_val WHERE mapContainsValue(m, toLowCardinality('V0\0'));
SELECT 'mapContainsKey LowCardinality needle',   groupArray(id), (SELECT groupArray(id) FROM t_needle_map_key WHERE arrayExists(x -> x = toLowCardinality('V0\0'), mapKeys(m))) FROM t_needle_map_key WHERE mapContainsKey(m, toLowCardinality('V0\0')) SETTINGS optimize_functions_to_subcolumns = 0;
DROP TABLE t_needle_map_val;
DROP TABLE t_needle_map_key;

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
DROP TABLE t_map_val_lc_def;
DROP TABLE t_map_lc_def;
DROP TABLE t_lc_null_def;
DROP TABLE t_lc_nfs3_def;
