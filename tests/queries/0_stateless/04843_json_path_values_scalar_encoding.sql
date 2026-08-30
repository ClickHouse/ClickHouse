SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS json_index_tokens_float_equality;
CREATE TABLE json_index_tokens_float_equality
(
    id UInt64,
    data JSON(f32 Float32, f64 Float64, nonzero Float64, floats Array(Float64), nested_floats Array(Tuple(Float64))),
    INDEX json_tokens data TYPE text(tokenizer = jsonPathValues(64))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_index_tokens_float_equality;
INSERT INTO json_index_tokens_float_equality VALUES
    (1, '{"f32":-0.0,"f64":-0.0,"nonzero":1.5,"floats":[-0.0],"nested_floats":[[-0.0]]}'),
    (2, '{"f32":0.0,"f64":0.0,"nonzero":2.5,"floats":[0.0],"nested_floats":[[0.0]]}');

SELECT 'signed zero';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_float_equality WHERE data.f32 = toFloat32(0.0)
SETTINGS force_data_skipping_indices = 'json_tokens';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_float_equality WHERE data.f64 = toFloat64(-0.0)
SETTINGS force_data_skipping_indices = 'json_tokens';

SELECT 'nonzero float';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_float_equality WHERE data.nonzero = toFloat64(1.5)
SETTINGS force_data_skipping_indices = 'json_tokens';

SELECT 'structured float equality fallback';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_float_equality WHERE data.floats = [toFloat64(0.0)];
SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_float_equality WHERE data.floats = [toFloat64(0.0)]
)
WHERE position(explain, '__text_index') > 0;
SELECT arraySort(groupArray(id)) FROM json_index_tokens_float_equality WHERE data.nested_floats = [(toFloat64(0.0),)];
SELECT count() = 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_float_equality WHERE data.nested_floats = [(toFloat64(0.0),)]
)
WHERE position(explain, '__text_index') > 0;

SELECT 'signed zero direct read plan';
SELECT count() > 0
FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM json_index_tokens_float_equality WHERE data.f64 = toFloat64(0.0)
    SETTINGS query_plan_optimize_count_from_text_index = 0
)
WHERE position(explain, '__text_index') > 0;

DROP TABLE json_index_tokens_float_equality;

CREATE TABLE json_index_tokens_tiny_limit
(
    id UInt64,
    data JSON(s String),
    INDEX json_tokens data TYPE text(tokenizer = jsonPathValues(1)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;
INSERT INTO json_index_tokens_tiny_limit VALUES (1, '{"s":"value"}');
SELECT arraySort(groupArray(id)) FROM json_index_tokens_tiny_limit WHERE data.s = 'value';
SELECT arraySort(groupArray(id)) FROM json_index_tokens_tiny_limit WHERE data.s = 'value'
SETTINGS force_data_skipping_indices = 'json_tokens'; -- { serverError INDEX_NOT_USED }
CHECK TABLE json_index_tokens_tiny_limit SETTINGS check_query_single_value_result = 1;
DROP TABLE json_index_tokens_tiny_limit;

CREATE TABLE json_index_tokens_tiny_pattern_limit
(
    id UInt64,
    data JSON(s String),
    INDEX json_tokens data TYPE text(tokenizer = jsonPathValues(8)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;
INSERT INTO json_index_tokens_tiny_pattern_limit VALUES (1, '{"s":"long pattern value"}');
SELECT arraySort(groupArray(id)) FROM json_index_tokens_tiny_pattern_limit WHERE startsWith(data.s, 'long');
SELECT arraySort(groupArray(id)) FROM json_index_tokens_tiny_pattern_limit WHERE startsWith(data.s, 'long')
SETTINGS force_data_skipping_indices = 'json_tokens'; -- { serverError INDEX_NOT_USED }
CHECK TABLE json_index_tokens_tiny_pattern_limit SETTINGS check_query_single_value_result = 1;
DROP TABLE json_index_tokens_tiny_pattern_limit;
-- FixedString values compare zero-padded ('a' = toFixedString('a', 3)), but `jsonPathValues`
-- tokens store exact bytes. The text index must not prune rows for predicates with
-- FixedString needles on String or Dynamic paths.

SET use_skip_indexes = 1;

DROP TABLE IF EXISTS json_pv_fixed_string_padding;
CREATE TABLE json_pv_fixed_string_padding
(
    id UInt64,
    json JSON,
    INDEX idx json TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_pv_fixed_string_padding;
INSERT INTO json_pv_fixed_string_padding VALUES (1, '{"k":"a"}'), (2, '{"k":"ab"}'), (3, '{"k":"b"}');

SELECT count() FROM json_pv_fixed_string_padding WHERE json.k.:String IN (SELECT toFixedString('a', 3));
SELECT count() FROM json_pv_fixed_string_padding WHERE json.k.:String = toFixedString('a', 3);
SELECT count() FROM json_pv_fixed_string_padding WHERE json.k = toFixedString('a', 3);
SELECT count() FROM json_pv_fixed_string_padding WHERE has(['a', 'b']::Array(FixedString(3)), json.k.:String);

-- The same predicates must return identical results without the index.
SELECT count() FROM json_pv_fixed_string_padding WHERE json.k.:String IN (SELECT toFixedString('a', 3)) SETTINGS use_skip_indexes = 0;
SELECT count() FROM json_pv_fixed_string_padding WHERE json.k.:String = toFixedString('a', 3) SETTINGS use_skip_indexes = 0;
SELECT count() FROM json_pv_fixed_string_padding WHERE json.k = toFixedString('a', 3) SETTINGS use_skip_indexes = 0;
SELECT count() FROM json_pv_fixed_string_padding WHERE has(['a', 'b']::Array(FixedString(3)), json.k.:String) SETTINGS use_skip_indexes = 0;

DROP TABLE json_pv_fixed_string_padding;

DROP TABLE IF EXISTS json_pv_fixed_string_width;
CREATE TABLE json_pv_fixed_string_width
(
    id UInt64,
    json JSON(k FixedString(3)),
    INDEX idx json TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_pv_fixed_string_width;
INSERT INTO json_pv_fixed_string_width VALUES
    (1, '{"k":"a"}'),
    (2, '{"k":"b"}'),
    (3, '{"k":"c"}');

-- Equal-width `FixedString` values have identical padding, so the index remains usable.
SELECT count() FROM json_pv_fixed_string_width
WHERE json.k IN (SELECT toFixedString('a', 3))
SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_pv_fixed_string_width
WHERE json.k = toFixedString('a', 3)
SETTINGS force_data_skipping_indices = 'idx';

-- Different widths compare equal after zero-padding, but their token bytes differ. The index must
-- decline this predicate instead of pruning the matching row.
SELECT count() FROM json_pv_fixed_string_width
WHERE json.k IN (SELECT toFixedString('a', 4));
SELECT count() FROM json_pv_fixed_string_width
WHERE json.k IN (SELECT toFixedString('a', 4))
SETTINGS use_skip_indexes = 0;

DROP TABLE json_pv_fixed_string_width;

DROP TABLE IF EXISTS json_pv_fixed_string_to_string;
CREATE TABLE json_pv_fixed_string_to_string
(
    id UInt64,
    json JSON(k FixedString(3), s String),
    INDEX idx json TYPE text(tokenizer = jsonPathValues(64)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES json_pv_fixed_string_to_string;
INSERT INTO json_pv_fixed_string_to_string VALUES
    (1, '{"k":"a","s":"a"}'),
    (2, '{"k":"b","s":"b"}');

-- `toString` removes `FixedString` padding, so it is not transparent to the index.
SELECT count() FROM json_pv_fixed_string_to_string WHERE toString(json.k) = unhex('610000');
SELECT count() FROM json_pv_fixed_string_to_string WHERE toString(json.k) = unhex('610000')
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM json_pv_fixed_string_to_string WHERE toString(json.k) IN ('a');
SELECT count() FROM json_pv_fixed_string_to_string WHERE toString(json.k) IN ('a')
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM json_pv_fixed_string_to_string WHERE has(['a'], toString(json.k));
SELECT count() FROM json_pv_fixed_string_to_string WHERE has(['a'], toString(json.k))
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

-- Plain `String` remains transparent.
SELECT count() FROM json_pv_fixed_string_to_string WHERE toString(json.s) IN ('a')
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_pv_fixed_string_to_string;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS json_path_values_equality_safety;
CREATE TABLE json_path_values_equality_safety
(
    id UInt64,
    data JSON(
        flag Nullable(Bool),
        flags Array(Nullable(Bool)),
        i8 Int8,
        u8 UInt8,
        d Decimal64(1),
        ds Array(Decimal64(1)),
        ip IPv4,
        implicit_dt DateTime,
        implicit_dts Array(DateTime),
        implicit_dt64 DateTime64(3),
        explicit_dt DateTime('UTC'),
        explicit_dts Array(DateTime('UTC')),
        tuple_i Tuple(x Int64),
        tuples_i Array(Tuple(x Int64))),
    INDEX tokens data TYPE text(tokenizer = jsonPathValues) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_equality_safety VALUES
    (1, '{"flag":true,"flags":[true],"i8":127,"u8":255,"d":33.3,"ds":[33.3],"ip":"1.2.3.4","implicit_dt":"2030-01-01 00:00:00","implicit_dts":["2030-01-01 00:00:00"],"implicit_dt64":"2030-01-01 00:00:00.000","explicit_dt":"2030-01-01 00:00:00","explicit_dts":["2030-01-01 00:00:00"],"tuple_i":{"x":7},"tuples_i":[{"x":7}]}'),
    (2, '{"flag":false,"flags":[false],"i8":-128,"u8":0,"d":12.0,"ds":[12.0],"ip":"8.8.8.8","implicit_dt":"2031-01-01 00:00:00","implicit_dts":["2031-01-01 00:00:00"],"implicit_dt64":"2031-01-01 00:00:00.000","explicit_dt":"2031-01-01 00:00:00","explicit_dts":["2031-01-01 00:00:00"],"tuple_i":{"x":8},"tuples_i":[{"x":8}]}');

SELECT 'exact scalar equality', arraySort(groupArray(id)) FROM json_path_values_equality_safety
WHERE data.flag = 1
SETTINGS force_data_skipping_indices = 'tokens';
SELECT 'exact array element', arraySort(groupArray(id)) FROM json_path_values_equality_safety
WHERE has(data.flags, 1)
SETTINGS force_data_skipping_indices = 'tokens';
SELECT 'exact decimal', arraySort(groupArray(id)) FROM json_path_values_equality_safety
WHERE data.d = toDecimal64(33.3, 1)
SETTINGS force_data_skipping_indices = 'tokens';
SELECT 'exact decimal array', arraySort(groupArray(id)) FROM json_path_values_equality_safety
WHERE has(data.ds, toDecimal64(33.3, 1))
SETTINGS force_data_skipping_indices = 'tokens';
SELECT 'exact integer conversion', arraySort(groupArray(id)) FROM json_path_values_equality_safety
WHERE data.i8 = 127
SETTINGS force_data_skipping_indices = 'tokens';
SELECT 'exact IPv4 conversion', arraySort(groupArray(id)) FROM json_path_values_equality_safety
WHERE data.ip = toUInt32(16909060)
SETTINGS force_data_skipping_indices = 'tokens';
SELECT 'explicit timezone scalar', arraySort(groupArray(id)) FROM json_path_values_equality_safety
WHERE data.explicit_dt = toDateTime('2030-01-01 00:00:00', 'UTC')
SETTINGS force_data_skipping_indices = 'tokens';
SELECT 'explicit timezone array', arraySort(groupArray(id)) FROM json_path_values_equality_safety
WHERE has(data.explicit_dts, toDateTime('2030-01-01 00:00:00', 'UTC'))
SETTINGS force_data_skipping_indices = 'tokens';
SELECT 'exact tuple', arraySort(groupArray(id)) FROM json_path_values_equality_safety
WHERE data.tuple_i = CAST(tuple(toInt64(7)), 'Tuple(x Int64)')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT 'lossy Bool equality', arraySort(groupArray(id)) FROM json_path_values_equality_safety
WHERE data.flag = 2
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM json_path_values_equality_safety
WHERE data.flag = 2
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'lossy Bool array', arraySort(groupArray(id)) FROM json_path_values_equality_safety
WHERE has(data.flags, 2)
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM json_path_values_equality_safety
WHERE has(data.flags, 2)
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_path_values_equality_safety
WHERE data.flag = CAST(2, 'Nullable(UInt8)')
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_path_values_equality_safety
WHERE has(data.flags, CAST(2, 'Nullable(UInt8)'))
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'integer overflow', arraySort(groupArray(id)) FROM json_path_values_equality_safety
WHERE data.i8 = 128
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM json_path_values_equality_safety
WHERE data.i8 = 128
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'unsigned underflow', arraySort(groupArray(id)) FROM json_path_values_equality_safety
WHERE data.u8 = -1
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM json_path_values_equality_safety
WHERE data.u8 = -1
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'lossy Decimal equality', arraySort(groupArray(id)) FROM json_path_values_equality_safety
WHERE data.d = toDecimal64(33.33, 2)
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM json_path_values_equality_safety
WHERE data.d = toDecimal64(33.33, 2)
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT 'lossy Decimal array', arraySort(groupArray(id)) FROM json_path_values_equality_safety
WHERE has(data.ds, toDecimal64(33.33, 2))
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM json_path_values_equality_safety
WHERE has(data.ds, toDecimal64(33.33, 2))
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM json_path_values_equality_safety
WHERE data.implicit_dt = toDateTime('2030-01-01 00:00:00')
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_path_values_equality_safety
WHERE has(data.implicit_dts, toDateTime('2030-01-01 00:00:00'))
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_path_values_equality_safety
WHERE data.implicit_dt64 = toDateTime64('2030-01-01 00:00:00.000', 3)
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'tuple coercion', arraySort(groupArray(id)) FROM json_path_values_equality_safety
WHERE data.tuple_i = tuple(toUInt64(7))
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT 'tuple coercion indexed', arraySort(groupArray(id)) FROM json_path_values_equality_safety
WHERE data.tuple_i = tuple(toUInt64(7))
SETTINGS force_data_skipping_indices = 'tokens';
SELECT count() FROM json_path_values_equality_safety
WHERE has(data.tuples_i, tuple(toInt64(7)))
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

DROP TABLE json_path_values_equality_safety;

DROP TABLE IF EXISTS json_path_values_dynamic_equality_safety;
CREATE TABLE json_path_values_dynamic_equality_safety
(
    id UInt64,
    data JSON,
    INDEX tokens data TYPE text(tokenizer = jsonPathValues) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_path_values_dynamic_equality_safety VALUES
    (1, '{"bool_value":true,"float_value":0.5,"number":42,"mixed":42}'),
    (2, '{"bool_value":false,"float_value":2.5,"number":7,"mixed":"zzz"}'),
    (3, '{"bool_value":false,"float_value":3.5,"number":8,"mixed":42.0}');

SELECT 'dynamic string Bool baseline', arraySort(groupArray(id)) FROM json_path_values_dynamic_equality_safety
WHERE data.bool_value = '1'
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0,
    dynamic_throw_on_type_mismatch = 0;
SELECT count() FROM json_path_values_dynamic_equality_safety
WHERE data.bool_value = '1'
SETTINGS force_data_skipping_indices = 'tokens', dynamic_throw_on_type_mismatch = 0; -- { serverError INDEX_NOT_USED }

SELECT 'dynamic string Float baseline', arraySort(groupArray(id)) FROM json_path_values_dynamic_equality_safety
WHERE data.float_value = '0.50'
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0,
    dynamic_throw_on_type_mismatch = 0;
SELECT count() FROM json_path_values_dynamic_equality_safety
WHERE data.float_value = '0.50'
SETTINGS force_data_skipping_indices = 'tokens', dynamic_throw_on_type_mismatch = 0; -- { serverError INDEX_NOT_USED }

SELECT 'dynamic exact numeric', arraySort(groupArray(id)) FROM json_path_values_dynamic_equality_safety
WHERE data.number = 42
SETTINGS force_data_skipping_indices = 'tokens', dynamic_throw_on_type_mismatch = 0;

SELECT 'dynamic numeric baseline', arraySort(groupArray(id)) FROM json_path_values_dynamic_equality_safety
WHERE data.mixed = 42
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0,
    dynamic_throw_on_type_mismatch = 0;
SELECT 'dynamic numeric indexed', arraySort(groupArray(id)) FROM json_path_values_dynamic_equality_safety
WHERE data.mixed = 42
SETTINGS force_data_skipping_indices = 'tokens', dynamic_throw_on_type_mismatch = 0;

DROP TABLE json_path_values_dynamic_equality_safety;
