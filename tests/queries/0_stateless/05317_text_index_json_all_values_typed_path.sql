-- { echo }
-- A `JSONAllValues` text index skips only granules without the text of a matching value of a typed JSON path,
-- when that path is compared with a constant of another type, through a cast, or with an array function.

-- A constant of another type, or a string, matches the value of the path's type: the index looks for its text.
DROP TABLE IF EXISTS t_bool;
CREATE TABLE t_bool (id UInt64, j JSON(b Bool), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_bool VALUES (1, '{"b": true}'), (2, '{"b": false}');
SELECT count() FROM t_bool WHERE j.b = 1 SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_bool WHERE j.b = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bool WHERE j.b = 1.0 SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_bool WHERE j.b = 1.0 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bool WHERE j.b = '1' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_bool WHERE j.b = '1' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bool WHERE j.b = true SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE IF EXISTS t_nullable_bool;
CREATE TABLE t_nullable_bool (id UInt64, j JSON(b Nullable(Bool)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_nullable_bool VALUES (1, '{"b": true}'), (2, '{"b": false}');
SELECT count() FROM t_nullable_bool WHERE j.b = 1 SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_nullable_bool WHERE j.b = 1 SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS t_subcolumn;
CREATE TABLE t_subcolumn (id UInt64, j JSON, INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_subcolumn VALUES (1, '{"x": true}'), (2, '{"x": "q"}');
SELECT count() FROM t_subcolumn WHERE j.x.:Bool = 1 SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_subcolumn WHERE j.x.:Bool = 1 SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS t_date;
CREATE TABLE t_date (id UInt64, j JSON(d Date), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_date VALUES (1, '{"d": "2020-01-01"}'), (2, '{"d": "2021-01-01"}');
SELECT count() FROM t_date WHERE j.d = '2020-1-1' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_date WHERE j.d = '2020-1-1' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_date WHERE j.d = '2020-01-01' SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE IF EXISTS t_int;
CREATE TABLE t_int (id UInt64, j JSON(n Int64), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_int VALUES (1, '{"n": 5}'), (2, '{"n": 7}');
SELECT count() FROM t_int WHERE j.n = '05' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_int WHERE j.n = '05' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_int WHERE j.n = 5 SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_int WHERE j.n = 5.0 SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE IF EXISTS t_uint;
CREATE TABLE t_uint (id UInt64, j JSON(n UInt64), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_uint VALUES (1, '{"n": 12345}'), (2, '{"n": 7}');
SELECT count() FROM t_uint WHERE j.n = 12345 SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE IF EXISTS t_float;
CREATE TABLE t_float (id UInt64, j JSON(f Float64), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_float VALUES (1, '{"f": -0.0}'), (2, '{"f": 1}'), (3, '{"f": 7}');
SELECT count() FROM t_float WHERE j.f = '1.0' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_float WHERE j.f = '1.0' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_float WHERE j.f = 1 SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE IF EXISTS t_uuid;
CREATE TABLE t_uuid (id UInt64, j JSON(u UUID), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_uuid VALUES (1, '{"u": "6ba7b810-9dad-11d1-80b4-00c04fd430c8"}'), (2, '{"u": "00000000-0000-0000-0000-000000000007"}');
SELECT count() FROM t_uuid WHERE j.u = '6BA7B810-9DAD-11D1-80B4-00C04FD430C8' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_uuid WHERE j.u = '6BA7B810-9DAD-11D1-80B4-00C04FD430C8' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_uuid WHERE j.u = '6ba7b810-9dad-11d1-80b4-00c04fd430c8' SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE IF EXISTS t_fixed_string;
CREATE TABLE t_fixed_string (id UInt64, j JSON(s FixedString(3)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = splitByString([' '])))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_fixed_string VALUES (1, '{"s": "a"}'), (2, '{"s": "zzz"}');
SELECT count() FROM t_fixed_string WHERE j.s = 'a' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_fixed_string WHERE j.s = 'a' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_fixed_string WHERE j.s = 'abcd';

DROP TABLE IF EXISTS t_fixed_string_in;
CREATE TABLE t_fixed_string_in (id UInt64, j JSON(s FixedString(3)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_fixed_string_in VALUES (1, '{"s": "a"}'), (2, '{"s": "zzz"}');
SELECT count() FROM t_fixed_string_in WHERE j.s IN ('a') SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE IF EXISTS t_date_time;
CREATE TABLE t_date_time (id UInt64, j JSON(dt DateTime('UTC')), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_date_time VALUES (1, '{"dt": "2020-01-01 00:00:00"}'), (2, '{"dt": "2021-01-01 10:00:00"}'), (3, '{"dt": "2022-02-02 00:00:00"}');
SELECT count() FROM t_date_time WHERE j.dt = '2021-01-01T10:00:00Z' SETTINGS cast_string_to_date_time_mode = 'best_effort', force_data_skipping_indices = 'idx';
SELECT count() FROM t_date_time WHERE j.dt = '2021-01-01T10:00:00Z' SETTINGS cast_string_to_date_time_mode = 'best_effort', use_skip_indexes = 0;

DROP TABLE IF EXISTS t_date_time64;
CREATE TABLE t_date_time64 (id UInt64, j JSON(dt DateTime64(3, 'UTC')), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_date_time64 VALUES (1, '{"dt": "2020-01-01 00:00:00.1"}'), (2, '{"dt": "2020-01-01 00:00:00"}'), (3, '{"dt": "2021-01-01 00:00:00.5"}'), (4, '{"dt": "2022-01-01 00:00:00"}');
SELECT count() FROM t_date_time64 WHERE j.dt = '2020-01-01 00:00:00.1' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_date_time64 WHERE j.dt = '2020-01-01 00:00:00.1' SETTINGS use_skip_indexes = 0;

DROP TABLE IF EXISTS t_array_date_time;
CREATE TABLE t_array_date_time (id UInt64, j JSON(a Array(DateTime('UTC'))), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_array_date_time VALUES (1, '{"a": ["2020-01-01 00:00:00"]}'), (2, '{"a": ["2021-01-01 00:00:00"]}');
SELECT count() FROM t_array_date_time WHERE j.a = '[\'2020-01-01 00:00:00\']' SETTINGS force_data_skipping_indices = 'idx';

-- Constants of the path's own type.
DROP TABLE IF EXISTS t_enum;
CREATE TABLE t_enum (id UInt64, j JSON(e Enum8('a' = 1, 'b' = 2)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_enum VALUES (1, '{"e": "a"}'), (2, '{"e": "b"}');
SELECT count() FROM t_enum WHERE j.e = 'a' SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE IF EXISTS t_decimal;
CREATE TABLE t_decimal (id UInt64, j JSON(p Decimal(10, 2)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_decimal VALUES (1, '{"p": 1.5}'), (2, '{"p": 7}');
SELECT count() FROM t_decimal WHERE j.p = 1.5::Decimal(10, 2) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE IF EXISTS t_array_int;
CREATE TABLE t_array_int (id UInt64, j JSON(a Array(Int64)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_array_int VALUES (1, '{"a": [7]}'), (2, '{"a": [8]}');
SELECT count() FROM t_array_int WHERE j.a = [7]::Array(Int64) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE IF EXISTS t_array_string;
CREATE TABLE t_array_string (id UInt64, j JSON(a Array(String)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_array_string VALUES (1, '{"a": ["a", "b"]}'), (2, '{"a": ["abc"]}'), (3, '{"a": ["zz"]}');
SELECT count() FROM t_array_string WHERE j.a = ['a', 'b'] SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE IF EXISTS t_map;
CREATE TABLE t_map (id UInt64, j JSON(m Map(String, UInt8)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_map VALUES (1, '{"m": {"k": 1}}'), (2, '{"m": {"z": 7}}');
SELECT count() FROM t_map WHERE j.m = map('k', 1) SETTINGS force_data_skipping_indices = 'idx';

-- A `String` path keeps the index for its string searches.
DROP TABLE IF EXISTS t_string;
CREATE TABLE t_string (id UInt64, j JSON(x String), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_string VALUES (1, '{"x": "01"}'), (2, '{"x": "1"}'), (3, '{"x": "7"}');
SELECT count() FROM t_string WHERE j.x = '1' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_string WHERE hasToken(j.x, '1') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_string WHERE j.x IN ('1', 'zz') SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE IF EXISTS t_low_cardinality;
CREATE TABLE t_low_cardinality (id UInt64, j JSON(x LowCardinality(String)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_low_cardinality VALUES (1, '{"x": "abc"}'), (2, '{"x": "zz"}');
SELECT count() FROM t_low_cardinality WHERE j.x = 'abc' SETTINGS force_data_skipping_indices = 'idx';

-- Comparisons whose matching values have other texts, or no single one, do not use the index.
SELECT count() FROM t_bool WHERE j.b::String = 'yes' SETTINGS bool_true_representation = 'yes';
SELECT count() FROM t_bool WHERE j.b::String = 'yes' SETTINGS bool_true_representation = 'yes', use_skip_indexes = 0;
SELECT count() FROM t_bool WHERE j.b::String = 'yes' SETTINGS bool_true_representation = 'yes', force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_bool WHERE j.b::String IN ('yes', 'zzz') SETTINGS bool_true_representation = 'yes';
SELECT count() FROM t_bool WHERE j.b::String IN ('yes', 'zzz') SETTINGS bool_true_representation = 'yes', use_skip_indexes = 0;
SELECT count() FROM t_bool WHERE j.b::String IN ('yes', 'zzz') SETTINGS bool_true_representation = 'yes', force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM t_string WHERE j.x::Float64 = 1;
SELECT count() FROM t_string WHERE j.x::Float64 = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_string WHERE j.x::Float64 = 1 SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM t_int WHERE j.n::String = '5';
SELECT count() FROM t_int WHERE j.n::String = '5' SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM t_float WHERE j.f = 0;
SELECT count() FROM t_float WHERE j.f = 0 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_float WHERE j.f = 0 SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM t_enum WHERE j.e = 1;
SELECT count() FROM t_enum WHERE j.e = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_enum WHERE j.e = 1 SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM t_date_time WHERE j.dt = 1577836800;
SELECT count() FROM t_date_time WHERE j.dt = 1577836800 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_date_time WHERE j.dt = 1577836800 SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_date_time WHERE j.dt = toDate('2020-01-01');
SELECT count() FROM t_date_time WHERE j.dt = toDate('2020-01-01') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_date_time WHERE j.dt = toDate('2020-01-01') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM t_date_time64 WHERE j.dt = 1577836800;
SELECT count() FROM t_date_time64 WHERE j.dt = 1577836800 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_date_time64 WHERE j.dt = 1577836800 SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_date_time64 WHERE j.dt = 1609459200.5;
SELECT count() FROM t_date_time64 WHERE j.dt = 1609459200.5 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_date_time64 WHERE j.dt = 1609459200.5 SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM t_array_date_time WHERE j.a = [toDate('2020-01-01')];
SELECT count() FROM t_array_date_time WHERE j.a = [toDate('2020-01-01')] SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_array_date_time WHERE j.a = [toDate('2020-01-01')] SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM t_decimal WHERE j.p = 1.5;
SELECT count() FROM t_decimal WHERE j.p = 1.5 SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM t_array_int WHERE j.a = [7];
SELECT count() FROM t_array_int WHERE j.a = [7] SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM t_array_string WHERE has(j.a, 'a');
SELECT count() FROM t_array_string WHERE has(j.a, 'a') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_array_string WHERE has(j.a, 'a') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_array_string WHERE hasAnyTokens(j.a, ['abc']);
SELECT count() FROM t_array_string WHERE hasAnyTokens(j.a, ['abc']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_array_string WHERE hasAnyTokens(j.a, ['abc']) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SELECT count() FROM t_map WHERE has(j.m, 'k');
SELECT count() FROM t_map WHERE has(j.m, 'k') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_map WHERE has(j.m, 'k') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE IF EXISTS t_array_string_escaped;
CREATE TABLE t_array_string_escaped (id UInt64, j JSON(a Array(String)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_array_string_escaped VALUES (1, '{"a": ["a\\nb"]}'), (2, '{"a": ["zz"]}');
SELECT count() FROM t_array_string_escaped WHERE has(j.a, 'a\nb');
SELECT count() FROM t_array_string_escaped WHERE has(j.a, 'a\nb') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_array_string_escaped WHERE has(j.a, 'a\nb') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE IF EXISTS t_ipv4;
CREATE TABLE t_ipv4 (id UInt64, j JSON(ip IPv4), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_ipv4 VALUES (1, '{"ip": "0.0.0.1"}'), (2, '{"ip": "7.7.7.7"}');
SELECT count() FROM t_ipv4 WHERE j.ip = 1;
SELECT count() FROM t_ipv4 WHERE j.ip = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_ipv4 WHERE j.ip = 1 SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE IF EXISTS t_decimal_float;
CREATE TABLE t_decimal_float (id UInt64, j JSON(p Decimal(18, 17)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_decimal_float VALUES (1, '{"p": "0.10000000000000001"}'), (2, '{"p": 7}');
SELECT count() FROM t_decimal_float WHERE j.p = 0.1;
SELECT count() FROM t_decimal_float WHERE j.p = 0.1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_decimal_float WHERE j.p = 0.1 SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE IF EXISTS t_array_float;
CREATE TABLE t_array_float (id UInt64, j JSON(a Array(Float64)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_array_float VALUES (1, '{"a": [-0.0]}'), (2, '{"a": [7]}');
SELECT count() FROM t_array_float WHERE j.a = [0.];
SELECT count() FROM t_array_float WHERE j.a = [0.] SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_array_float WHERE j.a = [0.] SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE IF EXISTS t_tuple;
CREATE TABLE t_tuple (id UInt64, j JSON(t Tuple(a Float64, b String)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_tuple VALUES (1, '{"t": {"a": -0.0, "b": "x"}}'), (2, '{"t": {"a": 7, "b": "z"}}');
SELECT count() FROM t_tuple WHERE j.t = '(0,\'x\')';
SELECT count() FROM t_tuple WHERE j.t = '(0,\'x\')' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_tuple WHERE j.t = '(0,\'x\')' SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

-- A nested `JSON` holds values of runtime types in its dynamic paths.
DROP TABLE IF EXISTS t_object;
CREATE TABLE t_object (id UInt64, j JSON(obj JSON(a UInt64)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_object VALUES (1, '{"obj": {"a": 1, "c": -0.0}}'), (2, '{"obj": {"a": 7}}'), (3, '{"obj": {"a": 1}}');
SELECT count() FROM t_object WHERE j.obj = '{"a":1,"c":0.0}';
SELECT count() FROM t_object WHERE j.obj = '{"a":1,"c":0.0}' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_object WHERE j.obj = '{"a":1,"c":0.0}' SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_object WHERE j.obj = '{"a":1}';
SELECT count() FROM t_object WHERE j.obj = '{"a":1}' SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE IF EXISTS t_object_ngrams;
CREATE TABLE t_object_ngrams (id UInt64, j JSON(obj JSON(a UInt64)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'ngrams'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_object_ngrams VALUES (1, '{"obj": {"a": 18446744073709551615}}'), (2, '{"obj": {"a": 7}}');
SELECT count() FROM t_object_ngrams WHERE CAST(j.obj AS String) = '{"a":"18446744073709551615"}' SETTINGS output_format_json_quote_64bit_integers = 1;
SELECT count() FROM t_object_ngrams WHERE CAST(j.obj AS String) = '{"a":"18446744073709551615"}' SETTINGS output_format_json_quote_64bit_integers = 1, use_skip_indexes = 0;
SELECT count() FROM t_object_ngrams WHERE CAST(j.obj AS String) = '{"a":"18446744073709551615"}' SETTINGS output_format_json_quote_64bit_integers = 1, force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE IF EXISTS t_object_variant;
CREATE TABLE t_object_variant (id UInt64, j JSON(obj JSON(a Variant(UInt64, String))), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_object_variant VALUES (1, '{"obj": {"a": 1, "c": -0.0}}'), (2, '{"obj": {"a": 7}}'), (3, '{"obj": {"a": 18446744073709551615}}');
SELECT count() FROM t_object_variant WHERE j.obj = '{"a":1,"c":0.0}';
SELECT count() FROM t_object_variant WHERE j.obj = '{"a":1,"c":0.0}' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_object_variant WHERE j.obj = '{"a":1,"c":0.0}' SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_object_variant WHERE CAST(j.obj AS String) IN ('{"a":"18446744073709551615"}', 'zz') SETTINGS output_format_json_quote_64bit_integers = 1;
SELECT count() FROM t_object_variant WHERE CAST(j.obj AS String) IN ('{"a":"18446744073709551615"}', 'zz') SETTINGS output_format_json_quote_64bit_integers = 1, use_skip_indexes = 0;
SELECT count() FROM t_object_variant WHERE CAST(j.obj AS String) IN ('{"a":"18446744073709551615"}', 'zz') SETTINGS output_format_json_quote_64bit_integers = 1, force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

-- A subcolumn of a typed path reads a part of the stored text.
DROP TABLE IF EXISTS t_sub_array;
CREATE TABLE t_sub_array (id UInt64, j JSON(arr Array(String)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_sub_array VALUES (1, '{"arr": []}'), (2, '{"arr": ["a"]}');
SELECT count() FROM t_sub_array WHERE empty(j.arr);
SELECT count() FROM t_sub_array WHERE empty(j.arr) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sub_array WHERE empty(j.arr) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE IF EXISTS t_sub_string;
CREATE TABLE t_sub_string (id UInt64, j JSON(s String), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_sub_string VALUES (1, '{"s": ""}'), (2, '{"s": "abc"}');
SELECT count() FROM t_sub_string WHERE empty(j.s);
SELECT count() FROM t_sub_string WHERE empty(j.s) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sub_string WHERE empty(j.s) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE IF EXISTS t_sub_tuple;
CREATE TABLE t_sub_tuple (id UInt64, j JSON(t Tuple(a Int64, b String)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_sub_tuple VALUES (1, '{"t": {"a": 1, "b": "x"}}'), (2, '{"t": {"a": 7, "b": "z"}}');
SELECT count() FROM t_sub_tuple WHERE j.t.a = 1;
SELECT count() FROM t_sub_tuple WHERE j.t.a = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sub_tuple WHERE j.t.a = 1 SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_sub_tuple WHERE j.t.b = 'x';
SELECT count() FROM t_sub_tuple WHERE j.t.b = 'x' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sub_tuple WHERE j.t.b = 'x' SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_sub_tuple WHERE j.t.b IN ('x', 'q');
SELECT count() FROM t_sub_tuple WHERE j.t.b IN ('x', 'q') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sub_tuple WHERE j.t.b IN ('x', 'q') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE IF EXISTS t_sub_object;
CREATE TABLE t_sub_object (id UInt64, j JSON(obj JSON(a UInt64)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_sub_object VALUES (1, '{"obj": {"a": 1}}'), (2, '{"obj": {"a": 7}}');
SELECT count() FROM t_sub_object WHERE j.obj.a = 1;
SELECT count() FROM t_sub_object WHERE j.obj.a = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sub_object WHERE j.obj.a = 1 SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE IF EXISTS t_sub_nullable;
CREATE TABLE t_sub_nullable (id UInt64, j JSON(n Nullable(Int64)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_sub_nullable VALUES (1, '{"n": null}'), (2, '{"n": 5}');
SELECT count() FROM t_sub_nullable WHERE j.n.null = 1;
SELECT count() FROM t_sub_nullable WHERE j.n.null = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sub_nullable WHERE j.n.null = 1 SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE IF EXISTS t_sub_dynamic;
CREATE TABLE t_sub_dynamic (id UInt64, j JSON, INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_sub_dynamic VALUES (1, '{"x": ["a"]}'), (2, '{"x": "q"}');
SELECT count() FROM t_sub_dynamic WHERE j.x.:`Array(Nullable(String))`.size0 = 1;
SELECT count() FROM t_sub_dynamic WHERE j.x.:`Array(Nullable(String))`.size0 = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sub_dynamic WHERE j.x.:`Array(Nullable(String))`.size0 = 1 SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE IF EXISTS t_tuple_json;
CREATE TABLE t_tuple_json (id UInt64, t Tuple(j JSON(n Int64)), INDEX idx JSONAllValues(t.j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_tuple_json VALUES (1, tuple('{"n": 5}')), (2, tuple('{"n": 7}'));
SELECT count() FROM t_tuple_json WHERE t.j.n = 5 SETTINGS force_data_skipping_indices = 'idx';

-- A `DateTime` without a time zone is written in the session time zone, while one with a time zone is not.
SET session_timezone = 'UTC';
DROP TABLE IF EXISTS t_implicit_zone;
CREATE TABLE t_implicit_zone (id UInt64, j JSON(dt DateTime), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_implicit_zone VALUES (1, '{"dt": "2030-02-02 00:00:00"}'), (2, '{"dt": "2031-03-03 00:00:00"}');
DROP TABLE IF EXISTS t_implicit_zone_array;
CREATE TABLE t_implicit_zone_array (id UInt64, j JSON(a Array(DateTime)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_implicit_zone_array VALUES (1, '{"a": ["2030-02-02 00:00:00"]}'), (2, '{"a": ["2031-03-03 00:00:00"]}');
DROP TABLE IF EXISTS t_implicit_zone64;
CREATE TABLE t_implicit_zone64 (id UInt64, j JSON(dt DateTime64(3)), INDEX idx JSONAllValues(j) TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_implicit_zone64 VALUES (1, '{"dt": "2030-02-02 00:00:00"}'), (2, '{"dt": "2031-03-03 00:00:00"}');
SELECT count() FROM t_implicit_zone WHERE j.dt = toDateTime('2030-02-02 00:00:00');
SELECT count() FROM t_implicit_zone WHERE j.dt = toDateTime('2030-02-02 00:00:00') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

SET session_timezone = 'Europe/Moscow';
SELECT count() FROM t_implicit_zone WHERE j.dt = toDateTime('2030-02-02 03:00:00');
SELECT count() FROM t_implicit_zone WHERE j.dt = toDateTime('2030-02-02 03:00:00') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_implicit_zone WHERE j.dt = toDateTime('2030-02-02 03:00:00') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_implicit_zone_array WHERE j.a = [toDateTime('2030-02-02 03:00:00')];
SELECT count() FROM t_implicit_zone_array WHERE j.a = [toDateTime('2030-02-02 03:00:00')] SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_implicit_zone_array WHERE j.a = [toDateTime('2030-02-02 03:00:00')] SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_implicit_zone64 WHERE j.dt = toDateTime64('2030-02-02 03:00:00', 3);
SELECT count() FROM t_implicit_zone64 WHERE j.dt = toDateTime64('2030-02-02 03:00:00', 3) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_implicit_zone64 WHERE j.dt = toDateTime64('2030-02-02 03:00:00', 3) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_date_time WHERE j.dt = '2020-01-01 00:00:00' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_date_time WHERE j.dt = '2020-01-01 00:00:00' SETTINGS use_skip_indexes = 0;

DROP TABLE t_bool;
DROP TABLE t_nullable_bool;
DROP TABLE t_subcolumn;
DROP TABLE t_date;
DROP TABLE t_int;
DROP TABLE t_uint;
DROP TABLE t_float;
DROP TABLE t_uuid;
DROP TABLE t_fixed_string;
DROP TABLE t_fixed_string_in;
DROP TABLE t_date_time;
DROP TABLE t_date_time64;
DROP TABLE t_array_date_time;
DROP TABLE t_enum;
DROP TABLE t_decimal;
DROP TABLE t_array_int;
DROP TABLE t_array_string;
DROP TABLE t_map;
DROP TABLE t_string;
DROP TABLE t_low_cardinality;
DROP TABLE t_array_string_escaped;
DROP TABLE t_ipv4;
DROP TABLE t_decimal_float;
DROP TABLE t_array_float;
DROP TABLE t_tuple;
DROP TABLE t_object;
DROP TABLE t_object_ngrams;
DROP TABLE t_object_variant;
DROP TABLE t_sub_array;
DROP TABLE t_sub_string;
DROP TABLE t_sub_tuple;
DROP TABLE t_sub_object;
DROP TABLE t_sub_nullable;
DROP TABLE t_sub_dynamic;
DROP TABLE t_tuple_json;
DROP TABLE t_implicit_zone;
DROP TABLE t_implicit_zone_array;
DROP TABLE t_implicit_zone64;
