-- Every assertion puts the function result next to the element-wise `=` oracle in the same row,
-- so no cell can pass without the two agreeing.
-- Oracles: arrayExists needs optimize_rewrite_array_exists_to_has = 0, otherwise it is rewritten
-- into the function under test.

SET allow_suspicious_variant_types = 1;
SET optimize_rewrite_array_exists_to_has = 0;

SELECT '-- Array(Dynamic), cross-width numeric needle: const, materialized, LowCardinality';

DROP TABLE IF EXISTS t_dyn_num;
CREATE TABLE t_dyn_num (id UInt8, v Array(Dynamic)) ENGINE = Memory;
INSERT INTO t_dyn_num VALUES (0, [1::UInt64]), (1, [2::UInt64]);

SELECT
    id,
    has(v, 1::UInt8) AS has_got,
    toUInt8(arrayExists(x -> x = 1::UInt8, v)) AS has_want,
    indexOf(v, 1::UInt8) AS index_of_got,
    indexOf(arrayMap(x -> toUInt8(x = 1::UInt8), v), 1) AS index_of_want,
    countEqual(v, 1::UInt8) AS count_equal_got,
    length(arrayFilter(x -> x = 1::UInt8, v)) AS count_equal_want,
    indexOfAssumeSorted(v, 1::UInt8) AS index_of_sorted_got
FROM t_dyn_num
ORDER BY id;

-- Const array: same relation as the materialized arm above.
SELECT
    has([1::UInt64::Dynamic], 1::UInt8) AS has_got,
    toUInt8(arrayExists(x -> x = 1::UInt8, [1::UInt64::Dynamic])) AS has_want,
    -- The result stays constant, so it does not depend on the row count.
    countDistinct(has([1::UInt64::Dynamic], 1::UInt8)) AS distinct_values_over_many_rows
FROM numbers(4);

-- LowCardinality elements, same answers as the non-LowCardinality twin.
DROP TABLE IF EXISTS t_lc_dyn;
CREATE TABLE t_lc_dyn (id UInt8, v Array(LowCardinality(String))) ENGINE = Memory;
INSERT INTO t_lc_dyn VALUES (0, ['a']), (1, ['b']);

SELECT id, has(v, 'a'::Dynamic) AS got, toUInt8(arrayExists(x -> x = 'a'::Dynamic, v)) AS want
FROM t_lc_dyn ORDER BY id;

-- A dictionary much larger than the number of selected elements takes the sparse branch of
-- dictionaryMatchesForSelectedIndexes, which requires a plain UInt8 result column.
DROP TABLE IF EXISTS t_lc_sparse;
CREATE TABLE t_lc_sparse (id UInt64, v Array(LowCardinality(String))) ENGINE = Memory;
INSERT INTO t_lc_sparse SELECT number, [toString(number)] FROM numbers(1000);

SELECT count() AS matching_rows, sum(assumeNotNull(want)) AS oracle_matching_rows
FROM (
    SELECT has(v, '7'::Dynamic) AS got, toUInt8(arrayExists(x -> x = '7'::Dynamic, v)) AS want
    FROM t_lc_sparse WHERE id < 3 OR id = 7
)
WHERE got = 1;

SELECT '-- Array(Dynamic), FixedString needle compared zero-padded, as `=` does';

DROP TABLE IF EXISTS t_dyn_fs;
CREATE TABLE t_dyn_fs (id UInt8, v Array(Dynamic)) ENGINE = Memory;
INSERT INTO t_dyn_fs VALUES (0, ['V0']), (1, ['V0\0']), (2, ['X']);

SELECT id, has(v, toFixedString('V0', 3)) AS got, toUInt8(arrayExists(x -> x = toFixedString('V0', 3), v)) AS want
FROM t_dyn_fs ORDER BY id;

SELECT
    countEqual(['V0', 'V0\0']::Array(Dynamic), toFixedString('V0', 3)) AS count_equal_got,
    length(arrayFilter(x -> x = toFixedString('V0', 3), ['V0', 'V0\0']::Array(Dynamic))) AS count_equal_want,
    indexOf(['V0', 'V0\0']::Array(Dynamic), toFixedString('V0', 3)) AS index_of_got,
    indexOf(arrayMap(x -> toUInt8(x = toFixedString('V0', 3)), ['V0', 'V0\0']::Array(Dynamic)), 1) AS index_of_want;

-- The non-erased twin keeps its current behaviour: its common type is String, not Dynamic.
SELECT
    countEqual(['V0', 'V0\0'], toFixedString('V0', 3)) AS count_equal,
    indexOf(['V0', 'V0\0'], toFixedString('V0', 3)) AS index_of;

SELECT '-- Array(Variant)';

DROP TABLE IF EXISTS t_var;
CREATE TABLE t_var (id UInt8, v Array(Variant(UInt8, UInt64))) ENGINE = Memory;
INSERT INTO t_var VALUES (0, [1::UInt64]), (1, [1::UInt8]), (2, [2::UInt64]);

SELECT
    id,
    has(v, CAST(1::UInt8, 'Variant(UInt8, UInt64)')) AS got,
    toUInt8(arrayExists(x -> x = CAST(1::UInt8, 'Variant(UInt8, UInt64)'), v)) AS want
FROM t_var ORDER BY id;

SELECT '-- erasure nested in Tuple, at any depth';

DROP TABLE IF EXISTS t_tuple1;
CREATE TABLE t_tuple1 (v Array(Tuple(Dynamic))) ENGINE = Memory;
INSERT INTO t_tuple1 VALUES ([tuple(1::UInt64)]);

SELECT has(v, tuple(1::UInt8)) AS got, toUInt8(arrayExists(x -> x = tuple(1::UInt8), v)) AS want FROM t_tuple1;

DROP TABLE IF EXISTS t_tuple2;
CREATE TABLE t_tuple2 (v Array(Tuple(Tuple(Dynamic)))) ENGINE = Memory;
INSERT INTO t_tuple2 VALUES ([tuple(tuple(1::UInt64))]);

SELECT has(v, tuple(tuple(1::UInt8))) AS got, toUInt8(arrayExists(x -> x = tuple(tuple(1::UInt8)), v)) AS want FROM t_tuple2;

DROP TABLE IF EXISTS t_tuple_var;
CREATE TABLE t_tuple_var (v Array(Tuple(Variant(UInt8, UInt64)))) ENGINE = Memory;
INSERT INTO t_tuple_var VALUES ([tuple(CAST(1::UInt64, 'Variant(UInt8, UInt64)'))]);

SELECT
    has(v, tuple(CAST(1::UInt8, 'Variant(UInt8, UInt64)'))) AS got,
    toUInt8(arrayExists(x -> x = tuple(CAST(1::UInt8, 'Variant(UInt8, UInt64)')), v)) AS want
FROM t_tuple_var;

SELECT '-- erasure behind Array/Map: equality for those containers is not decomposed, so no match';

DROP TABLE IF EXISTS t_arr_arr;
CREATE TABLE t_arr_arr (v Array(Array(Dynamic))) ENGINE = Memory;
INSERT INTO t_arr_arr VALUES ([[1::UInt64]]);

SELECT has(v, [1::UInt8]) AS got, toUInt8(arrayExists(x -> x = [1::UInt8], v)) AS want FROM t_arr_arr;

DROP TABLE IF EXISTS t_arr_map;
CREATE TABLE t_arr_map (v Array(Map(String, Dynamic))) ENGINE = Memory;
INSERT INTO t_arr_map VALUES ([map('k', 1::UInt64)]);

SELECT has(v, map('k', 1::UInt8)) AS got, toUInt8(arrayExists(x -> x = map('k', 1::UInt8), v)) AS want FROM t_arr_map;

DROP TABLE IF EXISTS t_tuple_arr;
CREATE TABLE t_tuple_arr (v Array(Tuple(Array(Dynamic)))) ENGINE = Memory;
INSERT INTO t_tuple_arr VALUES ([tuple([1::UInt64])]);

SELECT has(v, tuple([1::UInt8])) AS got, toUInt8(arrayExists(x -> x = tuple([1::UInt8]), v)) AS want FROM t_tuple_arr;

SELECT '-- NULL: two NULLs are equal, per has([NULL], NULL) -> 1';

DROP TABLE IF EXISTS t_null;
CREATE TABLE t_null (v Array(Dynamic)) ENGINE = Memory;
INSERT INTO t_null VALUES ([NULL::Dynamic]);

SELECT has(v, NULL::Dynamic) AS needle_dynamic_null, has(v, NULL) AS needle_bare_null FROM t_null;
SELECT has([NULL], NULL) AS const_bare, has([NULL::Dynamic], NULL::Dynamic) AS const_dynamic;

-- An array without NULLs does not match a NULL needle.
SELECT
    has(CAST(['x'], 'Array(Dynamic)'), NULL) AS got,
    toUInt8(arrayExists(y -> isNull(y), CAST(['x'], 'Array(Dynamic)'))) AS want;

-- A NULL needle against non-NULL erased values, and the reverse.
DROP TABLE IF EXISTS t_null_mix;
CREATE TABLE t_null_mix (id UInt8, v Array(Dynamic)) ENGINE = Memory;
INSERT INTO t_null_mix VALUES (0, [1::UInt64, NULL::Dynamic]), (1, [1::UInt64]);

SELECT id, has(v, NULL::Dynamic) AS null_needle, has(v, 1::UInt8) AS value_needle FROM t_null_mix ORDER BY id;

-- Array(Nullable(T)) with an erased needle keeps its wrapper-level NULL semantics.
DROP TABLE IF EXISTS t_nullable;
CREATE TABLE t_nullable (id UInt8, v Array(Nullable(String))) ENGINE = Memory;
INSERT INTO t_nullable VALUES (0, ['1']), (1, ['2']), (2, [NULL]);

SELECT id, has(v, '1'::Dynamic) AS got FROM t_nullable ORDER BY id;

SELECT '-- Map: direct has(map, key), mapContainsKey, mapContainsValue';

DROP TABLE IF EXISTS t_map_key;
CREATE TABLE t_map_key (id UInt8, m Map(Dynamic, UInt8)) ENGINE = Memory;
INSERT INTO t_map_key VALUES (0, map(1::UInt64, 7)), (1, map(1::UInt8, 7)), (2, map(2::UInt64, 7));

SELECT id, has(m, 1::UInt8) AS got, toUInt8(arrayExists(x -> x = 1::UInt8, mapKeys(m))) AS want
FROM t_map_key ORDER BY id;

-- mapContainsKey is rewritten to has(m.keys, ...) at the default optimize_functions_to_subcolumns = 1.
SELECT id, mapContainsKey(m, 1::UInt8) AS got, toUInt8(arrayExists(x -> x = 1::UInt8, mapKeys(m))) AS want
FROM t_map_key ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS t_map_value;
CREATE TABLE t_map_value (id UInt8, m Map(String, Dynamic)) ENGINE = Memory;
INSERT INTO t_map_value VALUES (0, map('k', 1::UInt64)), (1, map('k', 2::UInt64));

SELECT id, mapContainsValue(m, 1::UInt8) AS got, toUInt8(arrayExists(x -> x = 1::UInt8, mapValues(m))) AS want
FROM t_map_value ORDER BY id;

SELECT '-- non-erased elements with an erased needle: the common type decides, not the element type';

DROP TABLE IF EXISTS t_string;
CREATE TABLE t_string (id UInt8, v Array(String)) ENGINE = Memory;
INSERT INTO t_string VALUES (0, ['1']), (1, ['2']);

-- A String-valued Dynamic needle compares as a String.
SELECT id, has(v, '1'::Dynamic) AS got FROM t_string ORDER BY id;

-- A numeric Dynamic needle has no common type with String, so membership throws exactly like `=`.
SELECT has(v, 1::UInt64::Dynamic) FROM t_string; -- { serverError NO_COMMON_TYPE }
SELECT v[1] = 1::UInt64::Dynamic FROM t_string; -- { serverError NO_COMMON_TYPE }

-- With the mismatch setting disabled, `equals` yields NULL there and membership reports no match.
SELECT id, has(v, 1::UInt64::Dynamic) AS got FROM t_string ORDER BY id
SETTINGS dynamic_throw_on_type_mismatch = 0;

SELECT '-- mixed variants: the *_throw_on_type_mismatch settings are inherited from `equals`';

DROP TABLE IF EXISTS t_mixed_dyn;
CREATE TABLE t_mixed_dyn (id UInt8, v Array(Dynamic)) ENGINE = Memory;
INSERT INTO t_mixed_dyn VALUES (0, [1::UInt64]), (1, ['s']), (2, [1::UInt8]);

SELECT has(v, 1::UInt8) FROM t_mixed_dyn WHERE id = 1; -- { serverError NO_COMMON_TYPE }

SELECT id, has(v, 1::UInt8) AS got, toUInt8(arrayExists(x -> x = 1::UInt8, v)) AS want
FROM t_mixed_dyn ORDER BY id
SETTINGS dynamic_throw_on_type_mismatch = 0;

DROP TABLE IF EXISTS t_mixed_var;
CREATE TABLE t_mixed_var (id UInt8, v Array(Variant(String, UInt64))) ENGINE = Memory;
INSERT INTO t_mixed_var VALUES (0, [1::UInt64]), (1, ['s']);

SELECT
    id,
    has(v, CAST(1::UInt64, 'Variant(String, UInt64)')) AS got,
    toUInt8(arrayExists(x -> x = CAST(1::UInt64, 'Variant(String, UInt64)'), v)) AS want
FROM t_mixed_var ORDER BY id
SETTINGS variant_throw_on_type_mismatch = 0;

SELECT '-- non-erased fast paths are unchanged';

SELECT has([1, 2, 3], 2), indexOf([1, 2, 3], 3), countEqual([1, 2, 2], 2), indexOfAssumeSorted([1, 2, 3], 2);
SELECT has(['a', 'b'], 'b'), indexOf(['a', 'b'], 'a'), countEqual(['a', 'a'], 'a');
SELECT has([1, NULL, 3], NULL), has([1, 2, 3], NULL), has([toNullable(1), 2], 1);
SELECT has(map('a', 1, 'b', 2), 'b'), mapContainsKey(map('a', 1), 'a'), mapContainsValue(map('a', 1), 1);

DROP TABLE IF EXISTS t_dyn_num;
DROP TABLE IF EXISTS t_lc_dyn;
DROP TABLE IF EXISTS t_lc_sparse;
DROP TABLE IF EXISTS t_dyn_fs;
DROP TABLE IF EXISTS t_var;
DROP TABLE IF EXISTS t_tuple1;
DROP TABLE IF EXISTS t_tuple2;
DROP TABLE IF EXISTS t_tuple_var;
DROP TABLE IF EXISTS t_arr_arr;
DROP TABLE IF EXISTS t_arr_map;
DROP TABLE IF EXISTS t_tuple_arr;
DROP TABLE IF EXISTS t_null;
DROP TABLE IF EXISTS t_null_mix;
DROP TABLE IF EXISTS t_nullable;
DROP TABLE IF EXISTS t_map_key;
DROP TABLE IF EXISTS t_map_value;
DROP TABLE IF EXISTS t_string;
DROP TABLE IF EXISTS t_mixed_dyn;
DROP TABLE IF EXISTS t_mixed_var;
