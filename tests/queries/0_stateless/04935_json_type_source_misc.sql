-- The source subcolumn in various contexts.

SELECT 'subcolumn of the source';
SELECT json.__source.size FROM (SELECT materialize('{"a" : 42}')::JSON(with_source=1) AS json);

DROP TABLE IF EXISTS t_json_source_misc;
-- The size of the source is a separate stream only with these settings.
CREATE TABLE t_json_source_misc (json JSON(with_source=1)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS string_serialization_version = 'with_size_stream', propagate_types_serialization_versions_to_nested_types = 1, serialization_info_version = 'with_types';
INSERT INTO t_json_source_misc VALUES ('{"a" :  42}');

SELECT 'the source is listed in subcolumns';
DESCRIBE TABLE t_json_source_misc SETTINGS describe_include_subcolumns = 1 FORMAT TSVRaw;
SELECT arraySort(arrayFilter(x -> x LIKE '\_\_source%', subcolumns.names)) FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_json_source_misc' AND active AND column = 'json';

SELECT 'inserting into a type without the source does not create a path';
DROP TABLE IF EXISTS t_json_no_source;
CREATE TABLE t_json_no_source (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_no_source SELECT json FROM t_json_source_misc;
SELECT JSONAllPaths(json) FROM t_json_no_source;

SELECT 'inserting from a type without the source creates the text';
INSERT INTO t_json_source_misc SELECT json FROM t_json_no_source;
SELECT json.__source FROM t_json_source_misc ORDER BY json.__source;

DROP TABLE t_json_no_source;
DROP TABLE t_json_source_misc;

SELECT 'the text of a default row contains typed paths';
SELECT json, json.__source FROM format(JSONEachRow, 'json JSON(with_source=1, a UInt32)', '{"json" : null}') SETTINGS input_format_null_as_default = 1;
SELECT json, json.__source FROM format(JSONEachRow, 'json JSON(with_source=1)', '{"json" : null}') SETTINGS input_format_null_as_default = 1;

SELECT 'the text of a default row has the defaults of the types';
SELECT json, json.__source FROM format(JSONEachRow, 'json JSON(with_source=1, e Enum8(\'minus\' = -1, \'zero\' = 0))', '{"json" : null}') SETTINGS input_format_null_as_default = 1;
SELECT json, json.__source FROM format(JSONEachRow, 'json JSON(with_source=1, e Enum8(\'ok\' = 1))', '{"json" : null}') SETTINGS input_format_null_as_default = 1;
SELECT json, json.__source FROM (SELECT assumeNotNull(materialize(CAST(NULL, 'Nullable(JSON(with_source=1, e Enum8(\'minus\' = -1, \'zero\' = 0)))'))) AS json);

SELECT 'hidden default rows of a typed path whose column zero is not a value of its type';
DROP TABLE IF EXISTS t_json_source_enum_default;
CREATE TABLE t_json_source_enum_default (id UInt64, json JSON(with_source=1, e Enum8('a' = 1, 'b' = 2))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_source_enum_default VALUES (1, '{"e" : "a"}');
SELECT json, json.__source FROM t_json_source_enum_default;
DROP TABLE t_json_source_enum_default;
SELECT json FROM format(JSONEachRow, 'json Nullable(JSON(with_source=1, e Enum8(\'ok\' = 1)))', '{"json" : null}');
SELECT if(number = 0, materialize('{"e" : "ok"}')::JSON(with_source=1, e Enum8('ok' = 1)), NULL) AS json FROM numbers(2);
SELECT r.json FROM (SELECT 1 AS k) AS l LEFT JOIN (SELECT 2 AS k, '{"e" : "ok"}'::JSON(with_source=1, e Enum8('ok' = 1)) AS json) AS r USING k SETTINGS join_use_nulls = 1;

SELECT 'the common type keeps the source only if all types have it';
SELECT toTypeName(if(1, '{}'::JSON(with_source=1, a UInt32), '{}'::JSON(with_source=1, b UInt32)));
SELECT toTypeName(if(1, '{}'::JSON(with_source=1, max_dynamic_paths=8), '{}'::JSON(with_source=1, max_dynamic_paths=16)));
SELECT toTypeName(if(1, '{}'::JSON(with_source=1, a UInt32), '{}'::JSON(b UInt32)));

SELECT 'inside Nullable and Array';
DROP TABLE IF EXISTS t_json_source_containers;
CREATE TABLE t_json_source_containers (n Nullable(JSON(with_source=1)), a Array(JSON(with_source=1))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_source_containers VALUES ('{"x" :  1}', ['{"y" :  2}']), (NULL, []);
SELECT n.__source, arrayMap(x -> getSubcolumn(x, '__source'), a) FROM t_json_source_containers ORDER BY isNull(n);
DROP TABLE t_json_source_containers;

SELECT 'ALTER to the type with the source creates the text from existing data';
DROP TABLE IF EXISTS t_json_source_alter_data;
CREATE TABLE t_json_source_alter_data (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_source_alter_data VALUES ('{"a" :  42, "b" : "x"}');
ALTER TABLE t_json_source_alter_data MODIFY COLUMN json JSON(with_source=1) SETTINGS mutations_sync = 2;
SELECT json, json.__source FROM t_json_source_alter_data;
DROP TABLE t_json_source_alter_data;

SELECT 'CAST between the types with and without the source';
SELECT x, x.__source FROM (SELECT materialize('{"a" :  1}')::JSON::JSON(with_source=1) AS x);
SELECT toTypeName(x), x FROM (SELECT materialize('{"a" :  1}')::JSON(with_source=1)::JSON AS x);
SELECT 'CAST between the types with the source keeps the stored text';
SELECT x, x.__source FROM (SELECT materialize('{"a" :  1}')::JSON(with_source=1)::JSON(with_source=1, a UInt32) AS x);
SELECT x, x.__source FROM (SELECT materialize('{"a" : 1, "b" : 2}')::JSON(with_source=1)::JSON(with_source=1, SKIP b) AS x);
SELECT x, x.__source FROM (SELECT materialize('{"a" :  1}')::JSON(with_source=1)::JSON(with_source=1, a UInt32) AS x) SETTINGS json_use_optimized_type_conversion = 0;
SELECT x, x.__source FROM (SELECT accurateCastOrNull(materialize('{"a" :  1}')::JSON(with_source=1), 'JSON(with_source=1, a UInt32)') AS x);

SELECT 'a conversion keeps the parsed object, not the stored source, under either output setting';
SELECT JSONAllPaths(CAST(CAST(materialize('{"secret":1}') AS JSON(with_source=1, SKIP secret)) AS JSON)) SETTINGS output_format_json_type_use_source = 0;
SELECT JSONAllPaths(CAST(CAST(materialize('{"secret":1}') AS JSON(with_source=1, SKIP secret)) AS JSON)) SETTINGS output_format_json_type_use_source = 1;
SELECT CAST(materialize('{}')::JSON(with_source=1, a UInt32) AS JSON) SETTINGS output_format_json_type_use_source = 1;
SELECT JSONAllPaths(CAST(CAST(materialize(tuple('{"secret":1}')) AS Tuple(j JSON(with_source=1, SKIP secret))).j AS JSON)) SETTINGS output_format_json_type_use_source = 1;

SELECT 'skip indexes on paths and values are not used for the source';
DROP TABLE IF EXISTS t_json_source_index;
CREATE TABLE t_json_source_index
(
    json JSON(with_source=1),
    INDEX idx_paths JSONAllPaths(json) TYPE bloom_filter GRANULARITY 1,
    INDEX idx_values JSONAllValues(json) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_json_source_index VALUES ('{"a":1}'), ('{"b":2}');
SELECT count() FROM t_json_source_index WHERE json.__source = '{"a":1}' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_source_index WHERE json.__source = '{"a":1}' SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_json_source_index WHERE json.__source.size = 7 SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_json_source_index WHERE json.__source = '{"a":1}' SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'idx_paths'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM t_json_source_index WHERE json.__source = '{"a":1}' SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'idx_values'; -- { serverError INDEX_NOT_USED }
DROP TABLE t_json_source_index;

SELECT 'a column with only default paths is stored if its source is not the default text';
DROP TABLE IF EXISTS t_json_source_skip_empty;
CREATE TABLE t_json_source_skip_empty (id UInt64, json JSON(with_source=1, SKIP secret), tup Tuple(json JSON(with_source=1, SKIP secret)))
ENGINE = MergeTree ORDER BY id SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_json_source_skip_empty VALUES (1, '{ "secret" : "original" }', tuple('{ "secret" : "nested" }'));
SELECT json, json.__source, tup.json.__source FROM t_json_source_skip_empty;
DROP TABLE t_json_source_skip_empty;

SELECT 'generateRandom creates the source from the generated objects';
SELECT count(), countIf(isValidJSON(json.__source)) FROM (SELECT * FROM generateRandom('json JSON(with_source=1)', 42, 10, 3) LIMIT 100);
SELECT count(), countIf(arrayAll(s -> isValidJSON(s), arr.__source)) FROM (SELECT * FROM generateRandom('arr Array(JSON(with_source=1))', 42, 10, 3) LIMIT 100);

SELECT 'a default row of a JOIN has the defaults of the types in the value and in the source';
SELECT r.json, r.json.__source FROM (SELECT 1 AS k) AS l LEFT JOIN (SELECT 2 AS k, '{"e" : "zero"}'::JSON(with_source=1, e Enum8('minus' = -1, 'zero' = 0)) AS json) AS r USING k SETTINGS join_use_nulls = 0;

SELECT 'a reserved path from a reinterpreted aggregate state is rejected';
SELECT finalizeAggregation(CAST(unhex(h), 'AggregateFunction(groupUniqArray, JSON(with_source=1))'))
FROM (SELECT hex(groupUniqArrayState(j)) AS h FROM (SELECT materialize('{"__source" : 42, "a" : 1}')::JSON AS j)); -- { serverError INCORRECT_DATA }

SELECT 'a reserved path without values after a filter is dropped by CAST';
DROP TABLE IF EXISTS t_json_reserved_filtered;
CREATE TABLE t_json_reserved_filtered (j JSON) ENGINE = Memory;
INSERT INTO t_json_reserved_filtered VALUES ('{"__source" : 1}'), ('{"a" : 1}');
SELECT c, c.__source, JSONAllPaths(c) FROM (SELECT CAST(j AS JSON(with_source=1)) AS c FROM t_json_reserved_filtered WHERE isNull(j.__source));
SELECT CAST(j AS JSON(with_source=1)) FROM t_json_reserved_filtered; -- { serverError INCORRECT_DATA }
DROP TABLE t_json_reserved_filtered;

SELECT 'a Wide part is read with the source';
DROP TABLE IF EXISTS t_json_source_wide;
CREATE TABLE t_json_source_wide (id UInt64, json JSON(with_source=1)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_json_source_wide VALUES (1, '{ "a" : 1 }');
SELECT json, json.__source FROM t_json_source_wide;
DROP TABLE t_json_source_wide;

SELECT 'constants that differ only in the source keep their own source';
SELECT materialize('{"a":1}'::JSON(with_source=1)).__source, materialize('{ "a" : 1 }'::JSON(with_source=1)).__source;
SELECT tupleElement(materialize(tuple('{"a":1}'::JSON(with_source=1))), 1).__source, tupleElement(materialize(tuple('{ "a" : 1 }'::JSON(with_source=1))), 1).__source;
SELECT materialize(['{"a":1}'::JSON(with_source=1)])[1].__source, materialize(['{ "a" : 1 }'::JSON(with_source=1)])[1].__source;
-- The same holds when the constant's action name is hashed instead of spelled out.
SELECT materialize('{"a":1}'::JSON(with_source=1)).__source, materialize('{ "a" : 1 }'::JSON(with_source=1)).__source SETTINGS optimize_const_name_size = 0;

SELECT 'a value with the source is converted by parsing the source';
-- A removed SKIP rule brings the skipped path back, with and without the source in the destination.
SELECT j, JSONAllPaths(j), j.__source FROM (SELECT CAST('{"a" : 1, "b" : 2}'::JSON(with_source=1, SKIP b) AS JSON(with_source=1)) AS j);
SELECT j, JSONAllPaths(j) FROM (SELECT CAST('{"a" : 1, "b" : 2}'::JSON(with_source=1, SKIP b) AS JSON) AS j);
-- A value the source type could not hold is read again under the destination type.
SELECT j.a, j.__source FROM (SELECT CAST('{"a" : 300}'::JSON(with_source=1, a UInt8) AS JSON(with_source=1, a UInt32)) AS j) SETTINGS type_json_skip_invalid_typed_paths = 1;
SELECT j.a FROM (SELECT CAST('{"a" : "2020-01-01 00:00:00.123"}'::JSON(with_source=1, a DateTime('UTC')) AS JSON(with_source=1, a DateTime64(3, 'UTC'))) AS j);
