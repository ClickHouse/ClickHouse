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
    indexOfAssumeSorted(v, 1::UInt8) AS index_of_sorted_got,
    indexOf(arrayMap(x -> toUInt8(x = 1::UInt8), v), 1) AS index_of_sorted_want
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
INSERT INTO t_var VALUES (0, [1::UInt64]), (1, [2::UInt64]);

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

-- The same barrier when the declared type is a plain Dynamic and the container is only what it
-- happens to hold, which a check of the declared type alone would let through.
DROP TABLE IF EXISTS t_dyn_container;
CREATE TABLE t_dyn_container (id UInt8, v Array(Dynamic)) ENGINE = Memory;
INSERT INTO t_dyn_container VALUES (0, [CAST([1::UInt64::Dynamic], 'Dynamic')]),
    (1, [CAST(map('k', 1::UInt64), 'Dynamic')]), (2, [CAST(tuple([1::UInt64::Dynamic]), 'Dynamic')]);

SELECT has(v, CAST([1::UInt8::Dynamic], 'Dynamic')) AS array_alternative FROM t_dyn_container WHERE id = 0;
SELECT has(v, CAST(map('k', 1::UInt8), 'Dynamic')) AS map_alternative FROM t_dyn_container WHERE id = 1;
SELECT has(v, CAST(tuple([1::UInt8::Dynamic]), 'Dynamic')) AS tuple_of_array_alternative
FROM t_dyn_container WHERE id = 2;

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

-- A NULL needle whose nullness is reported by the column itself rather than by a wrapper, which is
-- how a LowCardinality dictionary reports it. All three spellings of the needle agree.
SET allow_suspicious_low_cardinality_types = 1;

SELECT
    has(materialize(CAST([NULL], 'Array(Dynamic)')), CAST(NULL, 'LowCardinality(Nullable(String))')) AS low_cardinality_needle,
    has(materialize(CAST([NULL], 'Array(Dynamic)')), CAST(NULL, 'Nullable(String)')) AS nullable_needle,
    has(materialize(CAST([NULL], 'Array(Dynamic)')), NULL) AS bare_needle,
    has(materialize(CAST(['x'], 'Array(Dynamic)')), CAST(NULL, 'LowCardinality(Nullable(String))')) AS no_match_control;

SET allow_suspicious_low_cardinality_types = 0;

-- A NULL needle against non-NULL erased values, and the reverse.
DROP TABLE IF EXISTS t_null_mix;
CREATE TABLE t_null_mix (id UInt8, v Array(Dynamic)) ENGINE = Memory;
INSERT INTO t_null_mix VALUES (0, [1::UInt64, NULL::Dynamic]), (1, [1::UInt64]);

SELECT id, has(v, NULL::Dynamic) AS null_needle, has(v, 1::UInt8) AS value_needle FROM t_null_mix ORDER BY id;

-- Rows where the match sits AFTER a NULL. The values of one stored type are held without the NULL
-- positions, so the reported position is what detects them being put back in the wrong place.
DROP TABLE IF EXISTS t_null_interleaved;
CREATE TABLE t_null_interleaved (id UInt8, v Array(Dynamic)) ENGINE = Memory;
INSERT INTO t_null_interleaved VALUES (0, [NULL, 1::UInt64]), (1, [NULL, 2::UInt64, 1::UInt64]), (2, [NULL, NULL]);

SELECT
    id,
    has(v, 1::UInt8) AS value_got,
    toUInt8(arrayExists(x -> x = 1::UInt8, v)) AS value_want,
    indexOf(v, 1::UInt8) AS value_index_got,
    indexOf(arrayMap(x -> toUInt8(x = 1::UInt8), v), 1) AS value_index_want,
    countEqual(v, 1::UInt8) AS value_count_got,
    length(arrayFilter(x -> x = 1::UInt8, v)) AS value_count_want,
    has(v, NULL) AS null_got,
    toUInt8(arrayExists(x -> isNull(x), v)) AS null_want,
    indexOf(v, NULL) AS null_index_got,
    indexOf(arrayMap(x -> toUInt8(isNull(x)), v), 1) AS null_index_want
FROM t_null_interleaved
ORDER BY id;

-- Array(Nullable(T)) with an erased needle keeps its wrapper-level NULL semantics.
DROP TABLE IF EXISTS t_nullable;
CREATE TABLE t_nullable (id UInt8, v Array(Nullable(String))) ENGINE = Memory;
INSERT INTO t_nullable VALUES (0, ['1']), (1, ['2']), (2, [NULL]);

SELECT id, has(v, '1'::Dynamic) AS got FROM t_nullable ORDER BY id;

SELECT '-- NULL nested in Tuple: invisible at the top level, so asserted at every depth';

-- Oracle note: the `=` oracle used everywhere else cannot express these rows. `=` over a Tuple
-- holding a NULL yields NULL, and arrayExists reads non-true as false, so it reports no match where
-- membership reports one. These rows therefore assert the null-safe relation `isNotDistinctFrom`,
-- which is what has([NULL], NULL) -> 1 means once the NULL sits inside a Tuple.

DROP TABLE IF EXISTS t_tuple_null;
CREATE TABLE t_tuple_null (id UInt8, v Array(Tuple(Dynamic))) ENGINE = Memory;
INSERT INTO t_tuple_null VALUES (0, [tuple(NULL::Dynamic)]), (1, [tuple('a'::Dynamic)]);

SELECT
    id,
    has(v, tuple(NULL::Dynamic)) AS null_needle_got,
    toUInt8(arrayExists(x -> isNotDistinctFrom(x, tuple(NULL::Dynamic)), v)) AS null_needle_want,
    has(v, tuple('a'::Dynamic)) AS value_needle_got,
    toUInt8(arrayExists(x -> isNotDistinctFrom(x, tuple('a'::Dynamic)), v)) AS value_needle_want,
    indexOf(v, tuple(NULL::Dynamic)) AS index_of_got,
    indexOf(arrayMap(x -> toUInt8(isNotDistinctFrom(x, tuple(NULL::Dynamic))), v), 1) AS index_of_want,
    countEqual(v, tuple(NULL::Dynamic)) AS count_equal_got,
    length(arrayFilter(x -> isNotDistinctFrom(x, tuple(NULL::Dynamic)), v)) AS count_equal_want
FROM t_tuple_null
ORDER BY id;

-- The const twin of the same relation.
SELECT
    has(CAST([tuple(NULL::Dynamic)], 'Array(Tuple(Dynamic))'), tuple(NULL::Dynamic)) AS null_pair,
    has(CAST([tuple(NULL::Dynamic)], 'Array(Tuple(Dynamic))'), tuple('a'::Dynamic)) AS null_vs_value,
    has(CAST([tuple('a'::Dynamic)], 'Array(Tuple(Dynamic))'), tuple(NULL::Dynamic)) AS value_vs_null;

-- Depth 2: the descent has to be unbounded, not one level.
DROP TABLE IF EXISTS t_tuple_null2;
CREATE TABLE t_tuple_null2 (v Array(Tuple(Tuple(Dynamic)))) ENGINE = Memory;
INSERT INTO t_tuple_null2 VALUES ([tuple(tuple(NULL::Dynamic))]);

SELECT
    has(v, tuple(tuple(NULL::Dynamic))) AS got,
    toUInt8(arrayExists(x -> isNotDistinctFrom(x, tuple(tuple(NULL::Dynamic))), v)) AS want,
    has(v, tuple(tuple('a'::Dynamic))) AS value_needle_got
FROM t_tuple_null2;

-- Variant erases types the same way Dynamic does.
DROP TABLE IF EXISTS t_tuple_null_var;
CREATE TABLE t_tuple_null_var (v Array(Tuple(Variant(UInt8, UInt64)))) ENGINE = Memory;
INSERT INTO t_tuple_null_var VALUES ([tuple(CAST(NULL, 'Variant(UInt8, UInt64)'))]);

SELECT
    has(v, tuple(CAST(NULL, 'Variant(UInt8, UInt64)'))) AS got,
    toUInt8(arrayExists(x -> isNotDistinctFrom(x, tuple(CAST(NULL, 'Variant(UInt8, UInt64)'))), v)) AS want,
    has(v, tuple(CAST(1::UInt8, 'Variant(UInt8, UInt64)'))) AS value_needle_got
FROM t_tuple_null_var;

-- The Tuple can also sit BELOW an erased wrapper rather than above it. Whether a wrapper's own
-- nullness is visible says nothing about a NULL nested under it, so every constructible wrapper
-- combination is enumerated here; a future one that behaves differently reddens a cell unprompted.
DROP TABLE IF EXISTS t_dyn_tuple_null;
CREATE TABLE t_dyn_tuple_null (id UInt8, v Array(Dynamic)) ENGINE = Memory;
INSERT INTO t_dyn_tuple_null VALUES (0, [CAST(tuple(NULL::Dynamic), 'Dynamic')]),
    (1, [CAST(tuple('a'::Dynamic), 'Dynamic')]);

SELECT
    id,
    has(v, CAST(tuple(NULL::Dynamic), 'Dynamic')) AS null_needle_got,
    toUInt8(arrayExists(x -> isNotDistinctFrom(x, CAST(tuple(NULL::Dynamic), 'Dynamic')), v)) AS null_needle_want,
    has(v, CAST(tuple('a'::Dynamic), 'Dynamic')) AS value_needle_got,
    toUInt8(arrayExists(x -> isNotDistinctFrom(x, CAST(tuple('a'::Dynamic), 'Dynamic')), v)) AS value_needle_want
FROM t_dyn_tuple_null
ORDER BY id;

DROP TABLE IF EXISTS t_var_tuple_null;
CREATE TABLE t_var_tuple_null (v Array(Variant(Tuple(Dynamic), UInt8))) ENGINE = Memory;
INSERT INTO t_var_tuple_null VALUES ([CAST(tuple(NULL::Dynamic), 'Variant(Tuple(Dynamic), UInt8)')]);

SELECT
    has(v, CAST(tuple(NULL::Dynamic), 'Variant(Tuple(Dynamic), UInt8)')) AS got,
    toUInt8(arrayExists(x -> isNotDistinctFrom(x, CAST(tuple(NULL::Dynamic), 'Variant(Tuple(Dynamic), UInt8)')), v)) AS want
FROM t_var_tuple_null;

-- Paired NULLs are not on their own a match: the non-NULL positions still have to agree.
DROP TABLE IF EXISTS t_tuple_null_mixed;
CREATE TABLE t_tuple_null_mixed (id UInt8, v Array(Tuple(Dynamic, UInt8))) ENGINE = Memory;
INSERT INTO t_tuple_null_mixed VALUES (0, [(NULL::Dynamic, 1::UInt8)]), (1, [(NULL::Dynamic, 2::UInt8)]),
    (2, [('a'::Dynamic, 1::UInt8)]);

SELECT
    id,
    has(v, (NULL::Dynamic, 1::UInt8)) AS null_first_got,
    toUInt8(arrayExists(x -> isNotDistinctFrom(x, (NULL::Dynamic, 1::UInt8)), v)) AS null_first_want,
    has(v, ('a'::Dynamic, 1::UInt8)) AS value_first_got,
    toUInt8(arrayExists(x -> isNotDistinctFrom(x, ('a'::Dynamic, 1::UInt8)), v)) AS value_first_want
FROM t_tuple_null_mixed
ORDER BY id;

SELECT '-- Map: direct has(map, key), mapContainsKey, mapContainsValue';

DROP TABLE IF EXISTS t_map_key;
CREATE TABLE t_map_key (id UInt8, m Map(Dynamic, UInt8)) ENGINE = Memory;
INSERT INTO t_map_key VALUES (0, map(1::UInt64, 7)), (1, map(2::UInt64, 7));

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

SELECT '-- out of contract: comparability that depends on the values, previous behaviour kept';

-- Every cell from here to the end of the next group is a must-not-regress control measured on
-- pristine master, not a cell this change fixes. The `=` oracle cannot express them: it throws as
-- soon as any one element is incomparable with the needle, while membership is an existential over
-- elements and must still answer for the elements that do compare.

DROP TABLE IF EXISTS t_string;
CREATE TABLE t_string (id UInt8, v Array(String)) ENGINE = Memory;
INSERT INTO t_string VALUES (0, ['1']), (1, ['2']);

-- A String-valued Dynamic needle compares as a String, which is already correct.
SELECT id, has(v, '1'::Dynamic) AS got FROM t_string ORDER BY id;

-- A numeric needle against String elements, under either value of the mismatch setting.
SELECT id, has(v, 1::UInt64::Dynamic) AS got FROM t_string ORDER BY id;
SELECT id, has(v, 1::UInt64::Dynamic) AS got FROM t_string ORDER BY id
SETTINGS dynamic_throw_on_type_mismatch = 0;

-- One concrete String alternative is not enough on its own: '1' parses as the needle and 'abc' does
-- not, so the column has no single answer.
DROP TABLE IF EXISTS t_str_alt;
CREATE TABLE t_str_alt (id UInt8, v Array(Dynamic)) ENGINE = Memory;
INSERT INTO t_str_alt VALUES (0, ['1']), (1, ['1', 'abc']);

SELECT id, has(v, 1::UInt8) AS got, indexOf(v, 1::UInt8) AS idx FROM t_str_alt ORDER BY id;

DROP TABLE IF EXISTS t_date_alt;
CREATE TABLE t_date_alt (v Array(Dynamic)) ENGINE = Memory;
INSERT INTO t_date_alt VALUES ([toDate('2020-01-01')]);

SELECT has(v, 1::UInt8) AS got FROM t_date_alt;

SELECT '-- out of contract: a single ROW holding several concrete types at once';

-- The scope is the row, not the column: a row whose own elements share one concrete type is fixed
-- even when other rows of the same column hold something else. Only the mixed ROW keeps master's
-- answer, where an element that cannot be compared with the needle must neither turn the whole row
-- into an error nor shift the position indexOf reports.
DROP TABLE IF EXISTS t_mixed_dyn;
CREATE TABLE t_mixed_dyn (id UInt8, v Array(Dynamic)) ENGINE = Memory;
INSERT INTO t_mixed_dyn VALUES (0, [1::UInt64]), (1, ['s', 1::UInt8]), (2, [1::UInt8]);

SELECT
    id,
    has(v, 1::UInt8) AS has_present,
    indexOf(v, 1::UInt8) AS idx,
    countEqual(v, 1::UInt8) AS cnt,
    has(v, 2::UInt8) AS has_absent,
    has(v, 1::UInt64) AS has_crosswidth
FROM t_mixed_dyn ORDER BY id;

SELECT
    has(['s'::Dynamic, 1::UInt8::Dynamic], 1::UInt8) AS c_has,
    indexOf(['s'::Dynamic, 1::UInt8::Dynamic], 1::UInt8) AS c_idx,
    countEqual(['s'::Dynamic, 1::UInt8::Dynamic, 1::UInt8::Dynamic], 1::UInt8) AS c_cnt,
    has(['s'::Dynamic, 1::UInt8::Dynamic], 2::UInt8) AS c_absent;

DROP TABLE IF EXISTS t_mixed_map;
CREATE TABLE t_mixed_map (m Map(Dynamic, UInt8)) ENGINE = Memory;
INSERT INTO t_mixed_map VALUES (map('s', 1, 1::UInt8, 2));

SELECT has(m, 1::UInt8) AS got FROM t_mixed_map;

DROP TABLE IF EXISTS t_mixed_var;
CREATE TABLE t_mixed_var (id UInt8, v Array(Variant(String, UInt64))) ENGINE = Memory;
INSERT INTO t_mixed_var VALUES (0, [1::UInt64]), (1, ['s']), (2, ['s', 1::UInt64]);

SELECT id, has(v, CAST(1::UInt64, 'Variant(String, UInt64)')) AS got FROM t_mixed_var ORDER BY id;

-- Values that overflow into the shared variant are serialised together under one discriminator, so a
-- row holding one is left alone. UInt8 claims the single real slot here, so row 1's matching UInt64
-- element overflows and its 0 is master's answer, not a fixed one; the shared column is asserted so
-- the row cannot silently stop exercising that path.
DROP TABLE IF EXISTS t_shared;
CREATE TABLE t_shared (id UInt8, v Array(Dynamic(max_types=1))) ENGINE = Memory;
INSERT INTO t_shared VALUES (0, [7::UInt8]), (1, [1::UInt64]), (2, ['s']);

SELECT id, has(v, 1::UInt8) AS got, arrayMap(e -> isDynamicElementInSharedData(e), v) AS shared
FROM t_shared ORDER BY id;

SELECT '-- the answer for a row never depends on which rows share its block';

-- This is the cell that pins block-independence. The same two rows are read under three different
-- block partitions; every partition must give the same answer, and the single-alternative row must
-- get the fixed answer (1) rather than the one its heterogeneous neighbour would force.
DROP TABLE IF EXISTS t_block_one;
CREATE TABLE t_block_one (id UInt8, v Array(Dynamic)) ENGINE = Memory;
INSERT INTO t_block_one VALUES (0, [1::UInt64]), (1, ['s', 1::UInt8]);

DROP TABLE IF EXISTS t_block_two;
CREATE TABLE t_block_two (id UInt8, v Array(Dynamic)) ENGINE = Memory;
INSERT INTO t_block_two VALUES (0, [1::UInt64]);
INSERT INTO t_block_two VALUES (1, ['s', 1::UInt8]);

SELECT 'one block   ', id, has(v, 1::UInt8) AS got FROM t_block_one ORDER BY id;
SELECT 'block size 1', id, has(v, 1::UInt8) AS got FROM t_block_one ORDER BY id SETTINGS max_block_size = 1;
SELECT 'two blocks  ', id, has(v, 1::UInt8) AS got FROM t_block_two ORDER BY id;

-- The needle is erased too, and its alternative varies by row: row 0 must still answer from its own
-- pair, so the three partitions have to agree here as well.
DROP TABLE IF EXISTS t_block_needle;
CREATE TABLE t_block_needle (id UInt8, v Array(Dynamic), n Dynamic) ENGINE = Memory;
INSERT INTO t_block_needle VALUES (0, [1::UInt64], 1::UInt8), (1, [2::UInt64], 'x');

DROP TABLE IF EXISTS t_block_needle_two;
CREATE TABLE t_block_needle_two (id UInt8, v Array(Dynamic), n Dynamic) ENGINE = Memory;
INSERT INTO t_block_needle_two VALUES (0, [1::UInt64], 1::UInt8);
INSERT INTO t_block_needle_two VALUES (1, [2::UInt64], 'x');

-- The oracle here is the row read on its own, not an expression in the same row: an element-wise
-- `x = n` over these rows asks for one common type across the column and throws NO_COMMON_TYPE, so
-- it would assert nothing. Reading each row alone is what "the answer is the row's own" means, and
-- the WHERE arm below is that reading. Every partition must produce it.
SELECT 'needle one block   ', id, has(v, n) AS got, indexOf(v, n) AS index_of_got, countEqual(v, n) AS count_equal_got
FROM t_block_needle ORDER BY id;
SELECT 'needle block size 1', id, has(v, n) AS got, indexOf(v, n) AS index_of_got, countEqual(v, n) AS count_equal_got
FROM t_block_needle ORDER BY id SETTINGS max_block_size = 1;
SELECT 'needle two blocks  ', id, has(v, n) AS got, indexOf(v, n) AS index_of_got, countEqual(v, n) AS count_equal_got
FROM t_block_needle_two ORDER BY id;
SELECT 'needle row alone   ', id, has(v, n) AS got, indexOf(v, n) AS index_of_got, countEqual(v, n) AS count_equal_got
FROM t_block_needle WHERE id = 0;
SELECT 'needle row alone   ', id, has(v, n) AS got, indexOf(v, n) AS index_of_got, countEqual(v, n) AS count_equal_got
FROM t_block_needle WHERE id = 1;

-- Same shape with the array written as a constant literal instead of a column, so the rows the
-- grouping leaves undecided are answered from that constant. `max_types = 1` leaves row 1's UInt8 in
-- the shared variant, so exactly one of the two rows is undecided and the block holds both kinds.
-- Here `=` on the same row is a usable oracle: the needle is compared against a constant, so no
-- common type is asked of the column and NO_COMMON_TYPE cannot arise.
DROP TABLE IF EXISTS t_block_const_array;
CREATE TABLE t_block_const_array (id UInt8, n Dynamic(max_types = 1)) ENGINE = Memory;
INSERT INTO t_block_const_array VALUES (0, 2::UInt64), (1, 1::UInt8);

SELECT 'const array one block   ', id, has([1::UInt64::Dynamic], n) AS got, indexOf([1::UInt64::Dynamic], n) AS index_of_got,
    countEqual([1::UInt64::Dynamic], n) AS count_equal_got, n = 1::UInt64::Dynamic AS equals_oracle
FROM t_block_const_array ORDER BY id;
SELECT 'const array block size 1', id, has([1::UInt64::Dynamic], n) AS got, indexOf([1::UInt64::Dynamic], n) AS index_of_got,
    countEqual([1::UInt64::Dynamic], n) AS count_equal_got, n = 1::UInt64::Dynamic AS equals_oracle
FROM t_block_const_array ORDER BY id SETTINGS max_block_size = 1;
SELECT 'const array row alone   ', id, has([1::UInt64::Dynamic], n) AS got, indexOf([1::UInt64::Dynamic], n) AS index_of_got,
    countEqual([1::UInt64::Dynamic], n) AS count_equal_got, n = 1::UInt64::Dynamic AS equals_oracle
FROM t_block_const_array WHERE id = 0;
SELECT 'const array row alone   ', id, has([1::UInt64::Dynamic], n) AS got, indexOf([1::UInt64::Dynamic], n) AS index_of_got,
    countEqual([1::UInt64::Dynamic], n) AS count_equal_got, n = 1::UInt64::Dynamic AS equals_oracle
FROM t_block_const_array WHERE id = 1;

-- Same shape with a declared Variant needle.
DROP TABLE IF EXISTS t_block_needle_variant;
CREATE TABLE t_block_needle_variant (id UInt8, v Array(Dynamic), n Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO t_block_needle_variant VALUES (0, [1::UInt64], 1::UInt64), (1, [2::UInt64], 'x'::String);

SELECT 'variant needle', id, has(v, n) AS got FROM t_block_needle_variant ORDER BY id;
SELECT 'variant needle size 1', id, has(v, n) AS got FROM t_block_needle_variant ORDER BY id SETTINGS max_block_size = 1;
SELECT 'variant needle alone', id, has(v, n) AS got FROM t_block_needle_variant WHERE id = 0;
SELECT 'variant needle alone', id, has(v, n) AS got FROM t_block_needle_variant WHERE id = 1;

-- The elements erase nothing and only the needle does, so the grouping key has to be built from the
-- needle side alone. The row-alone arm is the oracle, for the NO_COMMON_TYPE reason given above.
DROP TABLE IF EXISTS t_block_plain_elements;
CREATE TABLE t_block_plain_elements (id UInt8, v Array(UInt64), n Dynamic) ENGINE = Memory;
INSERT INTO t_block_plain_elements VALUES (0, [1], 1::UInt8), (1, [2], 'x');

DROP TABLE IF EXISTS t_block_plain_elements_two;
CREATE TABLE t_block_plain_elements_two (id UInt8, v Array(UInt64), n Dynamic) ENGINE = Memory;
INSERT INTO t_block_plain_elements_two VALUES (0, [1], 1::UInt8);
INSERT INTO t_block_plain_elements_two VALUES (1, [2], 'x');

SELECT 'plain elements one block   ', id, has(v, n) AS got, indexOf(v, n) AS index_of_got, countEqual(v, n) AS count_equal_got
FROM t_block_plain_elements ORDER BY id;
SELECT 'plain elements block size 1', id, has(v, n) AS got, indexOf(v, n) AS index_of_got, countEqual(v, n) AS count_equal_got
FROM t_block_plain_elements ORDER BY id SETTINGS max_block_size = 1;
SELECT 'plain elements two blocks  ', id, has(v, n) AS got, indexOf(v, n) AS index_of_got, countEqual(v, n) AS count_equal_got
FROM t_block_plain_elements_two ORDER BY id;
SELECT 'plain elements row alone   ', id, has(v, n) AS got, indexOf(v, n) AS index_of_got, countEqual(v, n) AS count_equal_got
FROM t_block_plain_elements WHERE id = 0;
SELECT 'plain elements row alone   ', id, has(v, n) AS got, indexOf(v, n) AS index_of_got, countEqual(v, n) AS count_equal_got
FROM t_block_plain_elements WHERE id = 1;

-- Nullable plain elements against the same varying erased needle, so the null maps are folded on the
-- grouped path and not only on the whole-block one.
DROP TABLE IF EXISTS t_block_plain_nullable;
CREATE TABLE t_block_plain_nullable (id UInt8, v Array(Nullable(UInt64)), n Dynamic) ENGINE = Memory;
INSERT INTO t_block_plain_nullable VALUES (0, [1, NULL], 1::UInt8), (1, [2], 'x'), (2, [NULL], NULL);

SELECT 'plain nullable one block   ', id, has(v, n) AS got FROM t_block_plain_nullable ORDER BY id;
SELECT 'plain nullable block size 1', id, has(v, n) AS got FROM t_block_plain_nullable ORDER BY id SETTINGS max_block_size = 1;
SELECT 'plain nullable row alone   ', id, has(v, n) AS got FROM t_block_plain_nullable WHERE id = 0;
SELECT 'plain nullable row alone   ', id, has(v, n) AS got FROM t_block_plain_nullable WHERE id = 1;
SELECT 'plain nullable row alone   ', id, has(v, n) AS got FROM t_block_plain_nullable WHERE id = 2;

-- The erased needle nested inside a Tuple, so the needle-side path is descended rather than taken at
-- the top level, with the element side wrapped to match.
DROP TABLE IF EXISTS t_block_needle_tuple;
CREATE TABLE t_block_needle_tuple (id UInt8, v Array(Tuple(Dynamic)), n Tuple(Dynamic)) ENGINE = Memory;
INSERT INTO t_block_needle_tuple VALUES (0, [tuple(1::UInt64)], tuple(1::UInt8)), (1, [tuple(2::UInt64)], tuple('x'));

SELECT 'tuple needle', id, has(v, n) AS got FROM t_block_needle_tuple ORDER BY id;
SELECT 'tuple needle size 1', id, has(v, n) AS got FROM t_block_needle_tuple ORDER BY id SETTINGS max_block_size = 1;
SELECT 'tuple needle alone', id, has(v, n) AS got FROM t_block_needle_tuple WHERE id = 0;
SELECT 'tuple needle alone', id, has(v, n) AS got FROM t_block_needle_tuple WHERE id = 1;

-- A peeled pair whose types have a supertype the comparison accepts but no conversion between them:
-- equals reports that as NOT_IMPLEMENTED rather than a type rejection, and declining has to cover it
-- too, or the call throws where it used to answer.
DROP TABLE IF EXISTS t_no_conversion;
CREATE TABLE t_no_conversion (v Array(Variant(IPv4, Float64)), n Variant(IPv4, Float64)) ENGINE = Memory;
INSERT INTO t_no_conversion VALUES ([toIPv4('1.2.3.4')], toFloat64(5));

SELECT 'no conversion', has(v, n) AS got FROM t_no_conversion;

-- An admitted cell answers the same under both values of the mismatch setting: the comparison sees
-- concrete types, so no adaptor is built and those settings do not reach it.
DROP TABLE IF EXISTS t_setting;
CREATE TABLE t_setting (v Array(Dynamic)) ENGINE = Memory;
INSERT INTO t_setting VALUES ([1::UInt64]);

SELECT has(v, 1::UInt8) AS got FROM t_setting SETTINGS dynamic_throw_on_type_mismatch = 0;
SELECT has(v, 1::UInt8) AS got FROM t_setting SETTINGS dynamic_throw_on_type_mismatch = 1;

SELECT '-- NULL nested inside a non-null Nullable(Tuple(...)) wrapper';

-- The wrapper's own nullness says nothing about a NULL nested below it, so both levels are
-- asserted here: rows 0-1 vary the nested value under a non-null wrapper, row 2 is a NULL wrapper.
-- The outer_* columns pair a NULL wrapper with a needle whose nested payload coincides with the
-- nested default, which is what pins that the wrapper's null map still counts.
-- Same isNotDistinctFrom oracle as the group above, for the same reason.
SET enable_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_null_tuple;
CREATE TABLE t_null_tuple (id UInt8, v Array(Nullable(Tuple(Dynamic)))) ENGINE = Memory;
INSERT INTO t_null_tuple VALUES (0, [tuple(NULL::Dynamic)]), (1, [tuple('a'::Dynamic)]), (2, [NULL]);

SELECT
    id,
    has(v, CAST(tuple(NULL::Dynamic), 'Nullable(Tuple(Dynamic))')) AS null_needle_got,
    toUInt8(arrayExists(x -> isNotDistinctFrom(x, CAST(tuple(NULL::Dynamic), 'Nullable(Tuple(Dynamic))')), v)) AS null_needle_want,
    has(v, CAST(tuple('a'::Dynamic), 'Nullable(Tuple(Dynamic))')) AS value_needle_got,
    toUInt8(arrayExists(x -> isNotDistinctFrom(x, CAST(tuple('a'::Dynamic), 'Nullable(Tuple(Dynamic))')), v)) AS value_needle_want,
    has(v, CAST(NULL, 'Nullable(Tuple(Dynamic))')) AS outer_null_needle_got,
    toUInt8(arrayExists(x -> isNotDistinctFrom(x, CAST(NULL, 'Nullable(Tuple(Dynamic))')), v)) AS outer_null_needle_want,
    indexOf(v, CAST(tuple(NULL::Dynamic), 'Nullable(Tuple(Dynamic))')) AS index_of_got,
    indexOf(arrayMap(x -> toUInt8(isNotDistinctFrom(x, CAST(tuple(NULL::Dynamic), 'Nullable(Tuple(Dynamic))'))), v), 1) AS index_of_want,
    countEqual(v, CAST(tuple(NULL::Dynamic), 'Nullable(Tuple(Dynamic))')) AS count_equal_got,
    length(arrayFilter(x -> isNotDistinctFrom(x, CAST(tuple(NULL::Dynamic), 'Nullable(Tuple(Dynamic))')), v)) AS count_equal_want
FROM t_null_tuple
ORDER BY id;

-- A NULL wrapper and a needle carrying the nested default must not match, in every function.
SELECT
    has(CAST([NULL], 'Array(Nullable(Tuple(Dynamic)))'), CAST(tuple(NULL::Dynamic), 'Nullable(Tuple(Dynamic))')) AS outer_vs_nested,
    has(CAST([tuple(NULL::Dynamic)], 'Array(Nullable(Tuple(Dynamic)))'), CAST(NULL, 'Nullable(Tuple(Dynamic))')) AS nested_vs_outer,
    has(CAST([NULL], 'Array(Nullable(Tuple(UInt8, Dynamic)))'), CAST(tuple(0, NULL::Dynamic), 'Nullable(Tuple(UInt8, Dynamic))')) AS outer_vs_defaults,
    indexOf(CAST([tuple('a'::Dynamic), NULL], 'Array(Nullable(Tuple(Dynamic)))'), CAST(tuple(NULL::Dynamic), 'Nullable(Tuple(Dynamic))')) AS index_of_skips_outer,
    countEqual(CAST([NULL, tuple(NULL::Dynamic), NULL], 'Array(Nullable(Tuple(Dynamic)))'), CAST(tuple(NULL::Dynamic), 'Nullable(Tuple(Dynamic))')) AS count_counts_nested_only;

-- Two NULL wrappers match each other; deeper nesting and the Variant twin behave the same.
SELECT
    has(CAST([NULL], 'Array(Nullable(Tuple(Dynamic)))'), CAST(NULL, 'Nullable(Tuple(Dynamic))')) AS outer_null_pair,
    has(CAST([tuple(tuple(NULL::Dynamic))], 'Array(Nullable(Tuple(Tuple(Dynamic))))'), CAST(tuple(tuple(NULL::Dynamic)), 'Nullable(Tuple(Tuple(Dynamic)))')) AS depth2,
    has(CAST([tuple(CAST(NULL, 'Variant(String, UInt64)'))], 'Array(Nullable(Tuple(Variant(String, UInt64))))'), CAST(tuple(CAST(NULL, 'Variant(String, UInt64)')), 'Nullable(Tuple(Variant(String, UInt64)))')) AS variant_twin;

SELECT '-- a Nullable(Tuple(...)) wrapper nested inside a Tuple still reaches the erased leaf';

-- The wrapper sits below the array element's top level here, so it is the per-level decay that has
-- to see through it. Materialized on purpose: the constant folder answers these without the
-- dispatcher. Each cell pairs the answer with scalar = on the same peeled pair, the authoritative
-- statement of intent for these shapes.
-- A non-matching leading element and a repeated match, so the position and the count each reduce to
-- something a constant could not stand in for.
DROP TABLE IF EXISTS t_nested_null_tuple;
CREATE TABLE t_nested_null_tuple (v Array(Tuple(Nullable(Tuple(Dynamic))))) ENGINE = Memory;
INSERT INTO t_nested_null_tuple VALUES ([tuple(tuple(9::UInt64::Dynamic)), tuple(tuple(1::UInt64::Dynamic)), tuple(tuple(1::UInt64::Dynamic))]);

SELECT
    has(v, tuple(CAST(tuple(1::UInt8::Dynamic), 'Nullable(Tuple(Dynamic))'))) AS got,
    toUInt8(arrayExists(x -> x = tuple(CAST(tuple(1::UInt8::Dynamic), 'Nullable(Tuple(Dynamic))')), v)) AS want,
    indexOf(v, tuple(CAST(tuple(1::UInt8::Dynamic), 'Nullable(Tuple(Dynamic))'))) AS index_of_got,
    indexOf(arrayMap(x -> toUInt8(x = tuple(CAST(tuple(1::UInt8::Dynamic), 'Nullable(Tuple(Dynamic))'))), v), 1) AS index_of_want,
    countEqual(v, tuple(CAST(tuple(1::UInt8::Dynamic), 'Nullable(Tuple(Dynamic))'))) AS count_equal_got,
    length(arrayFilter(x -> x = tuple(CAST(tuple(1::UInt8::Dynamic), 'Nullable(Tuple(Dynamic))')), v)) AS count_equal_want
FROM t_nested_null_tuple;

-- The same wrapper past a non-erased leading element, so the decay is exercised at a Tuple position
-- the traversal only reaches after skipping one.
DROP TABLE IF EXISTS t_nested_null_tuple_pos2;
CREATE TABLE t_nested_null_tuple_pos2 (v Array(Tuple(UInt8, Nullable(Tuple(Dynamic))))) ENGINE = Memory;
INSERT INTO t_nested_null_tuple_pos2 VALUES ([tuple(7, tuple(1::UInt64::Dynamic))]);

SELECT
    has(v, tuple(7::UInt8, CAST(tuple(1::UInt8::Dynamic), 'Nullable(Tuple(Dynamic))'))) AS got,
    toUInt8(v[1] = tuple(7::UInt8, CAST(tuple(1::UInt8::Dynamic), 'Nullable(Tuple(Dynamic))'))) AS want
FROM t_nested_null_tuple_pos2;

SELECT '-- a container under a Nullable(Tuple(...)) wrapper is decided by equals, not held back by the barrier';

-- The Array/Map barrier stops at a container the erased alternative names itself; a Nullable wrapper
-- ends that walk, so these rows are peeled and answered by `=`. Holding them back instead would
-- answer 0 for a materialized array while a constant one still matched numerically, so the three
-- regimes are asserted together, each against `=` on the same pair.
DROP TABLE IF EXISTS t_null_tuple_container;
CREATE TABLE t_null_tuple_container (id UInt8, v Array(Dynamic), n Dynamic) ENGINE = Memory;
INSERT INTO t_null_tuple_container
SELECT 0, [CAST(tuple(CAST(tuple([1::UInt64]), 'Nullable(Tuple(Array(UInt64)))')), 'Dynamic')],
          CAST(tuple(CAST(tuple([1::UInt8]), 'Nullable(Tuple(Array(UInt8)))')), 'Dynamic');
INSERT INTO t_null_tuple_container
SELECT 1, [CAST(tuple(CAST(tuple(map('k', 1::UInt64)), 'Nullable(Tuple(Map(String, UInt64)))')), 'Dynamic')],
          CAST(tuple(CAST(tuple(map('k', 1::UInt8)), 'Nullable(Tuple(Map(String, UInt8)))')), 'Dynamic');

SELECT 'hidden container materialized', id,
    has(v, n) AS got,
    toUInt8(arrayExists(x -> x = n, v)) AS want,
    indexOf(v, n) AS index_of_got,
    countEqual(v, n) AS count_equal_got
FROM t_null_tuple_container ORDER BY id;

SELECT 'hidden container row alone   ', id, has(v, n) AS got, toUInt8(arrayExists(x -> x = n, v)) AS want
FROM t_null_tuple_container WHERE id = 0;
SELECT 'hidden container row alone   ', id, has(v, n) AS got, toUInt8(arrayExists(x -> x = n, v)) AS want
FROM t_null_tuple_container WHERE id = 1;

SELECT 'hidden container const array ', id,
    has([CAST(tuple(CAST(tuple([1::UInt64]), 'Nullable(Tuple(Array(UInt64)))')), 'Dynamic')], n) AS got,
    toUInt8(arrayExists(x -> x = n, [CAST(tuple(CAST(tuple([1::UInt64]), 'Nullable(Tuple(Array(UInt64)))')), 'Dynamic')])) AS want
FROM t_null_tuple_container WHERE id = 0;

SET enable_nullable_tuple_type = 0;

SELECT '-- Map(LowCardinality(String), String): a NULL needle, whose dictionary has no null entry';

-- A Map key cannot be Nullable, so index 0 of its LowCardinality dictionary is the default value and
-- not a NULL. Each row pairs the Map answer with the plain array path over the same keys.
SELECT
    has(CAST(map('', 'v'), 'Map(LowCardinality(String), String)'), NULL) AS empty_key_got,
    has(mapKeys(CAST(map('', 'v'), 'Map(LowCardinality(String), String)')), NULL) AS empty_key_want,
    has(CAST(map('k', 'v'), 'Map(LowCardinality(String), String)'), NULL) AS non_empty_key_got,
    has(mapKeys(CAST(map('k', 'v'), 'Map(LowCardinality(String), String)')), NULL) AS non_empty_key_want;

-- Materialized, so the key column is a real dictionary rather than a constant.
SELECT has(m, NULL) AS got, has(mapKeys(m), NULL) AS want
FROM (SELECT CAST(map('', 'v'), 'Map(LowCardinality(String), String)') AS m);

SELECT
    mapContainsKey(CAST(map('', 'v'), 'Map(LowCardinality(String), String)'), NULL) AS empty_key,
    mapContainsKey(CAST(map('k', 'v'), 'Map(LowCardinality(String), String)'), NULL) AS non_empty_key
SETTINGS optimize_functions_to_subcolumns = 0;

-- Present and absent non-NULL needles: the ordinary LowCardinality Map path is undisturbed.
SELECT
    has(CAST(map('k', 'v'), 'Map(LowCardinality(String), String)'), 'k') AS present,
    has(CAST(map('k', 'v'), 'Map(LowCardinality(String), String)'), 'z') AS absent,
    mapContainsKey(CAST(map('k', 'v'), 'Map(LowCardinality(String), String)'), 'k') AS present_contains
SETTINGS optimize_functions_to_subcolumns = 0;

-- Control: the plain Array(LowCardinality(T)) path keeps the answer it has today. Its dictionary also
-- has no null entry, so this is the same question, but changing it is a separate user-visible change
-- on a non-erased type and is deliberately not part of this fix.
SELECT
    has(CAST(['', 'a'], 'Array(LowCardinality(String))'), NULL) AS has_with_empty,
    indexOf(CAST(['a', '', 'b'], 'Array(LowCardinality(String))'), NULL) AS index_of_with_empty,
    countEqual(CAST(['', 'a', ''], 'Array(LowCardinality(String))'), NULL) AS count_equal_with_empty,
    has(CAST(['a', 'b'], 'Array(LowCardinality(String))'), NULL) AS has_without_empty,
    has(CAST(['', NULL], 'Array(LowCardinality(Nullable(String)))'), NULL) AS nullable_dictionary;

SELECT '-- non-erased fast paths are unchanged';

SELECT has([1, 2, 3], 2), indexOf([1, 2, 3], 3), countEqual([1, 2, 2], 2), indexOfAssumeSorted([1, 2, 3], 2);
SELECT has(['a', 'b'], 'b'), indexOf(['a', 'b'], 'a'), countEqual(['a', 'a'], 'a');
SELECT has([1, NULL, 3], NULL), has([1, 2, 3], NULL), has([toNullable(1), 2], 1);
SELECT has(map('a', 1, 'b', 2), 'b'), mapContainsKey(map('a', 1), 'a'), mapContainsValue(map('a', 1), 1);

SELECT '-- constant array, varying needle: the rows are paired against the array in batches, so the answer must not depend on where a batch ends';

DROP TABLE IF EXISTS t_const_batched;
CREATE TABLE t_const_batched (n UInt64) ENGINE = Memory;
INSERT INTO t_const_batched SELECT number FROM numbers(300);

-- 250 elements over 300 rows: more rows than one batch holds, so several batches run per block.
SELECT
    countIf(has(a, n) != arrayExists(x -> x = n, a)) AS has_mismatches,
    countIf(indexOf(a, n) != arrayFirstIndex(x -> x = n, a)) AS index_of_mismatches,
    countIf(countEqual(a, n) != arrayCount(x -> x = n, a)) AS count_equal_mismatches,
    sum(has(a, n)) AS matched_rows
FROM (SELECT n, arrayMap(x -> x::UInt64::Dynamic, range(0, 1000, 4)) AS a FROM t_const_batched);

-- Needles carrying different alternatives per row: a batch of them cannot be compared as a whole, so
-- each such batch is grouped on its own rather than the whole block being reprocessed. A needle whose
-- alternative the comparison cannot relate to the elements at all is declined, and a declined row
-- keeps the answer the existing dispatch gives it, which against a constant array compares Fields and
-- so still matches numerically. The three arms take the batched path, the grouped one and neither, and
-- all of them have to answer alike.
SELECT 'mixed needles batched  ',
    sum(has(a, d)) AS matched_rows,
    sumIf(has(a, d), dynamicType(d) = 'UInt64') AS matched_same_alternative,
    sumIf(has(a, d), dynamicType(d) = 'String') AS matched_other_alternative
FROM (
    SELECT if(n % 2, (n - 1)::String::Dynamic, n::UInt64::Dynamic) AS d,
           arrayMap(x -> x::UInt64::Dynamic, range(0, 1000, 2)) AS a
    FROM t_const_batched
);
SELECT 'mixed needles grouped  ',
    sum(has(a, d)) AS matched_rows,
    sumIf(has(a, d), dynamicType(d) = 'UInt64') AS matched_same_alternative,
    sumIf(has(a, d), dynamicType(d) = 'String') AS matched_other_alternative
FROM (
    SELECT if(n % 2, (n - 1)::String::Dynamic, n::UInt64::Dynamic) AS d,
           arrayMap(x -> x::UInt64::Dynamic, range(0, 1000, 2)) AS a
    FROM t_const_batched
) SETTINGS max_block_size = 40;
SELECT 'mixed needles row alone',
    sum(has(a, d)) AS matched_rows,
    sumIf(has(a, d), dynamicType(d) = 'UInt64') AS matched_same_alternative,
    sumIf(has(a, d), dynamicType(d) = 'String') AS matched_other_alternative
FROM (
    SELECT if(n % 2, (n - 1)::String::Dynamic, n::UInt64::Dynamic) AS d,
           arrayMap(x -> x::UInt64::Dynamic, range(0, 1000, 2)) AS a
    FROM t_const_batched
) SETTINGS max_block_size = 1;

-- A constant array holding two alternatives cannot be compared as a whole batch, and its fallback
-- keeps comparing the constant, so a UInt8 needle still reaches a UInt64 element. 251 elements fit
-- 100 rows in one batch but not 300, so the two row counts must answer alike.
WITH arrayConcat(arrayMap(x -> x::UInt64::Dynamic, range(0, 1000, 4)), ['s'::Dynamic]) AS a
SELECT
    (SELECT DISTINCT has(a, materialize(4::UInt8)) FROM numbers(100)) AS has_one_batch,
    (SELECT DISTINCT has(a, materialize(4::UInt8)) FROM numbers(300)) AS has_several_batches,
    (SELECT DISTINCT indexOf(a, materialize(4::UInt8)) FROM numbers(300)) AS index_of_several_batches,
    (SELECT DISTINCT countEqual(a, materialize(4::UInt8)) FROM numbers(300)) AS count_equal_several_batches;

SELECT '-- notHas resolves has through the same factory, so the two stay negations of one another';

-- The JSON arm needs the setting: without it the typed path counts as present and both answers are
-- the same whichever resolver notHas holds.
SELECT has(json, 'a') AS json_has, notHas(json, 'a') AS json_not_has
FROM (SELECT CAST('{"b": 2}', 'JSON(a Nullable(Int64))') AS json)
SETTINGS type_json_skip_null_typed_paths = 1;

WITH arrayConcat(arrayMap(x -> x::UInt64::Dynamic, range(0, 1000, 4)), ['s'::Dynamic]) AS a
SELECT DISTINCT has(a, materialize(4::UInt8)) AS array_has, notHas(a, materialize(4::UInt8)) AS array_not_has
FROM numbers(300);

SELECT '-- arrayExists rewritten into has: the rewrite builds its own resolver, which must reach the same path';

DROP TABLE IF EXISTS t_rewritten;
CREATE TABLE t_rewritten (a Array(Dynamic)) ENGINE = Memory;
INSERT INTO t_rewritten VALUES (['V0'::String::Dynamic]);

-- The rewrite is an analyzer pass: without `enable_analyzer = 1` it never runs, so the second arm
-- would answer 1 on any build.
SELECT
    count() > 0 AS rewrite_happened
FROM (EXPLAIN QUERY TREE SELECT arrayExists(x -> x = toFixedString('V0', 3), a) FROM t_rewritten)
WHERE explain ILIKE '%has%'
SETTINGS optimize_rewrite_array_exists_to_has = 1, enable_analyzer = 1;

SELECT arrayExists(x -> x = toFixedString('V0', 3), a) AS rewritten_got
FROM t_rewritten
SETTINGS optimize_rewrite_array_exists_to_has = 1, enable_analyzer = 1;

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
DROP TABLE IF EXISTS t_shared;
DROP TABLE IF EXISTS t_block_one;
DROP TABLE IF EXISTS t_block_two;
DROP TABLE IF EXISTS t_block_const_array;
DROP TABLE IF EXISTS t_block_plain_elements;
DROP TABLE IF EXISTS t_block_plain_elements_two;
DROP TABLE IF EXISTS t_block_plain_nullable;
DROP TABLE IF EXISTS t_null_tuple;
DROP TABLE IF EXISTS t_const_batched;
DROP TABLE IF EXISTS t_rewritten;
