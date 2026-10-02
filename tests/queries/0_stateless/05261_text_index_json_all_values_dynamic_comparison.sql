-- A `JSONAllValues` text index holds the default text of each value, while a value of a `Dynamic` or `Variant` path is
-- compared in its own type after converting the constant (a stored `true` equals `1`), and its cast to `String` follows
-- the query's format settings. Only a string search on the path itself, or a typed subcolumn, may skip rows.

DROP TABLE IF EXISTS t_dynamic;
DROP TABLE IF EXISTS t_typed;
DROP TABLE IF EXISTS t_tokens;

CREATE TABLE t_dynamic (id UInt64, j JSON, INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_dynamic VALUES
    (1, '{"x": 1.5}'), (2, '{"x": 2.5}'), (3, '{"x": 1.0}'), (4, '{"x": 1e20}'), (5, '{"x": 0.1}'), (6, '{"x": true}'), (7, '{"x": -3}'),
    (8, '{"a": [true]}'), (9, '{"a": [1]}'), (10, '{"a": [2.5]}'),
    (11, '{"s": "hello"}'), (12, '{"s": 42}'), (13, '{"s": "one two three"}');

SELECT arraySort(groupUniqArray(dynamicType(j.x))) FROM t_dynamic WHERE j.x IS NOT NULL;

-- Rows 3 and 6 (`1.0` and `true`) match the first four comparisons, rows 8 and 9 (`[true]`, `[1]`) the next two, row 6 the last two.
SELECT count() FROM t_dynamic WHERE j.x = 1.0;
SELECT count() FROM t_dynamic WHERE j.x = 1.0 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_dynamic WHERE j.x = true;
SELECT count() FROM t_dynamic WHERE j.x = true SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_dynamic WHERE j.x = '1';
SELECT count() FROM t_dynamic WHERE j.x = '1' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_dynamic WHERE j.x::Float64 = 1;
SELECT count() FROM t_dynamic WHERE j.x::Float64 = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_dynamic WHERE has(j.a::Array(Float64), 1);
SELECT count() FROM t_dynamic WHERE has(j.a::Array(Float64), 1) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_dynamic WHERE startsWith(j.a, [1]);
SELECT count() FROM t_dynamic WHERE startsWith(j.a, [1]) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_dynamic WHERE j.x::String = 'yes' SETTINGS bool_true_representation = 'yes';
SELECT count() FROM t_dynamic WHERE j.x::String = 'yes' SETTINGS bool_true_representation = 'yes', use_skip_indexes = 0;
SELECT count() FROM t_dynamic WHERE j.x::String IN ('yes', 'zzz') SETTINGS bool_true_representation = 'yes';
SELECT count() FROM t_dynamic WHERE j.x::String IN ('yes', 'zzz') SETTINGS bool_true_representation = 'yes', use_skip_indexes = 0;
SELECT count() FROM t_dynamic WHERE j.x = 1.0 SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

-- A string search on the path and the `.:String` subcolumn read only `String` values, so they still use the index.
SELECT count() FROM t_dynamic WHERE hasToken(j.s, 'hello') SETTINGS dynamic_throw_on_type_mismatch = 0, force_data_skipping_indices = 'idx';
SELECT count() FROM t_dynamic WHERE hasToken(j.s, 'hello') SETTINGS dynamic_throw_on_type_mismatch = 0, use_skip_indexes = 0;
SELECT count() FROM t_dynamic WHERE hasTokenOrNull(j.s, 'two') SETTINGS dynamic_throw_on_type_mismatch = 0, force_data_skipping_indices = 'idx';
SELECT count() FROM t_dynamic WHERE match(j.s, 'one two three') SETTINGS dynamic_throw_on_type_mismatch = 0, force_data_skipping_indices = 'idx';
SELECT count() FROM t_dynamic WHERE j.s ILIKE '%THREE%' SETTINGS use_text_index_like_evaluation_by_dictionary_scan = 1, dynamic_throw_on_type_mismatch = 0, force_data_skipping_indices = 'idx';
SELECT count() FROM t_dynamic WHERE j.s.:String = 'hello' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_dynamic WHERE j.s.:String IN ('hello', 'zzz') SETTINGS force_data_skipping_indices = 'idx';

CREATE TABLE t_typed (id UInt64, j JSON(v Variant(Bool, Float64), t Tuple(a Dynamic), f Float64, w Variant(String, Int64)),
    INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_typed VALUES (1, '{"v": true, "t": {"a": true}, "f": 2.5, "w": "one two three"}'), (2, '{"v": 7.5, "t": {"a": 7.5}, "f": 3.5, "w": 7}');

-- Row 1 matches both: its `Bool` alternative and its `Dynamic` element hold `true`.
SELECT count() FROM t_typed WHERE j.v = 1;
SELECT count() FROM t_typed WHERE j.v = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_typed WHERE j.t = tuple(1);
SELECT count() FROM t_typed WHERE j.t = tuple(1) SETTINGS use_skip_indexes = 0;

-- A path of one type still uses the index.
SELECT count() FROM t_typed WHERE j.f = 3.5 SETTINGS force_data_skipping_indices = 'idx';

-- A string search on a `Variant` path still uses the index (on a bare `Dynamic` path `hasPhrase` is not a valid filter).
SELECT count() FROM t_typed WHERE hasPhrase(j.w, 'two three') SETTINGS variant_throw_on_type_mismatch = 0, force_data_skipping_indices = 'idx';

CREATE TABLE t_tokens (id UInt64, j JSON(v Variant(Array(String), String)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_tokens VALUES (1, '{"v": ["abc"]}'), (2, '{"v": "xyz"}');

-- `hasAllTokens` also takes an array, whose elements are its tokens, while the index holds the text `['abc']`.
SELECT count() FROM t_tokens WHERE hasAllTokens(j.v, 'abc', 'array');
SELECT count() FROM t_tokens WHERE hasAllTokens(j.v, 'abc', 'array') SETTINGS use_skip_indexes = 0;

DROP TABLE t_dynamic;
DROP TABLE t_typed;
DROP TABLE t_tokens;
