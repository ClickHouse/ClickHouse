SET enable_json_type = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 1;
SET use_skip_indexes_on_data_read = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS json_path_values_equality_safety;
CREATE TABLE json_path_values_equality_safety
(
    id UInt64,
    data JSON(
        flag Bool,
        flags Array(Bool),
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

SELECT 'exact scalar equality', groupArray(id) FROM json_path_values_equality_safety
WHERE data.flag = 1
SETTINGS force_data_skipping_indices = 'tokens';
SELECT 'exact array element', groupArray(id) FROM json_path_values_equality_safety
WHERE has(data.flags, 1)
SETTINGS force_data_skipping_indices = 'tokens';
SELECT 'exact decimal', groupArray(id) FROM json_path_values_equality_safety
WHERE data.d = toDecimal64(33.3, 1)
SETTINGS force_data_skipping_indices = 'tokens';
SELECT 'exact decimal array', groupArray(id) FROM json_path_values_equality_safety
WHERE has(data.ds, toDecimal64(33.3, 1))
SETTINGS force_data_skipping_indices = 'tokens';
SELECT 'exact integer conversion', groupArray(id) FROM json_path_values_equality_safety
WHERE data.i8 = 127
SETTINGS force_data_skipping_indices = 'tokens';
SELECT 'exact IPv4 conversion', groupArray(id) FROM json_path_values_equality_safety
WHERE data.ip = toUInt32(16909060)
SETTINGS force_data_skipping_indices = 'tokens';
SELECT 'explicit timezone scalar', groupArray(id) FROM json_path_values_equality_safety
WHERE data.explicit_dt = toDateTime('2030-01-01 00:00:00', 'UTC')
SETTINGS force_data_skipping_indices = 'tokens';
SELECT 'explicit timezone array', groupArray(id) FROM json_path_values_equality_safety
WHERE has(data.explicit_dts, toDateTime('2030-01-01 00:00:00', 'UTC'))
SETTINGS force_data_skipping_indices = 'tokens';
SELECT 'exact tuple', groupArray(id) FROM json_path_values_equality_safety
WHERE data.tuple_i = CAST(tuple(toInt64(7)), 'Tuple(x Int64)')
SETTINGS force_data_skipping_indices = 'tokens';

SELECT 'lossy Bool equality', groupArray(id) FROM json_path_values_equality_safety
WHERE data.flag = 2
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM json_path_values_equality_safety
WHERE data.flag = 2
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'lossy Bool array', groupArray(id) FROM json_path_values_equality_safety
WHERE has(data.flags, 2)
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM json_path_values_equality_safety
WHERE has(data.flags, 2)
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'integer overflow', groupArray(id) FROM json_path_values_equality_safety
WHERE data.i8 = 128
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM json_path_values_equality_safety
WHERE data.i8 = 128
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'unsigned underflow', groupArray(id) FROM json_path_values_equality_safety
WHERE data.u8 = -1
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM json_path_values_equality_safety
WHERE data.u8 = -1
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }

SELECT 'lossy Decimal equality', groupArray(id) FROM json_path_values_equality_safety
WHERE data.d = toDecimal64(33.33, 2)
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM json_path_values_equality_safety
WHERE data.d = toDecimal64(33.33, 2)
SETTINGS force_data_skipping_indices = 'tokens'; -- { serverError INDEX_NOT_USED }
SELECT 'lossy Decimal array', groupArray(id) FROM json_path_values_equality_safety
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

SELECT 'tuple coercion', groupArray(id) FROM json_path_values_equality_safety
WHERE data.tuple_i = tuple(toUInt64(7))
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0;
SELECT 'tuple coercion indexed', groupArray(id) FROM json_path_values_equality_safety
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

SELECT 'dynamic string Bool baseline', groupArray(id) FROM json_path_values_dynamic_equality_safety
WHERE data.bool_value = '1'
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0,
    dynamic_throw_on_type_mismatch = 0;
SELECT count() FROM json_path_values_dynamic_equality_safety
WHERE data.bool_value = '1'
SETTINGS force_data_skipping_indices = 'tokens', dynamic_throw_on_type_mismatch = 0; -- { serverError INDEX_NOT_USED }

SELECT 'dynamic string Float baseline', groupArray(id) FROM json_path_values_dynamic_equality_safety
WHERE data.float_value = '0.50'
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0,
    dynamic_throw_on_type_mismatch = 0;
SELECT count() FROM json_path_values_dynamic_equality_safety
WHERE data.float_value = '0.50'
SETTINGS force_data_skipping_indices = 'tokens', dynamic_throw_on_type_mismatch = 0; -- { serverError INDEX_NOT_USED }

SELECT 'dynamic exact numeric', groupArray(id) FROM json_path_values_dynamic_equality_safety
WHERE data.number = 42
SETTINGS force_data_skipping_indices = 'tokens', dynamic_throw_on_type_mismatch = 0;

SELECT 'dynamic numeric baseline', groupArray(id) FROM json_path_values_dynamic_equality_safety
WHERE data.mixed = 42
SETTINGS use_skip_indexes_on_data_read = 0, query_plan_direct_read_from_text_index = 0,
    dynamic_throw_on_type_mismatch = 0;
SELECT 'dynamic numeric indexed', groupArray(id) FROM json_path_values_dynamic_equality_safety
WHERE data.mixed = 42
SETTINGS force_data_skipping_indices = 'tokens', dynamic_throw_on_type_mismatch = 0;

DROP TABLE json_path_values_dynamic_equality_safety;
