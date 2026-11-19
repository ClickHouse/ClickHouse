-- The source subcolumn in various contexts.

SELECT 'subcolumn of the source';
SELECT json.__source.size FROM (SELECT materialize('{"a" : 42}')::JSON(with_source=1) AS json);

DROP TABLE IF EXISTS t_json_source_misc;
-- The size of the source is a separate stream only with these settings.
CREATE TABLE t_json_source_misc (json JSON(with_source=1)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS string_serialization_version = 'with_size_stream', propagate_types_serialization_versions_to_nested_types = 1;
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

SELECT 'the text of a default row has the defaults of the types, not the zeroes of the columns';
SELECT json, json.__source FROM format(JSONEachRow, 'json JSON(with_source=1, e Enum8(\'minus\' = -1, \'zero\' = 0))', '{"json" : null}') SETTINGS input_format_null_as_default = 1;
SELECT json, json.__source FROM format(JSONEachRow, 'json JSON(with_source=1, e Enum8(\'ok\' = 1))', '{"json" : null}') SETTINGS input_format_null_as_default = 1;
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

SELECT 'generateRandom creates the source from the generated objects';
SELECT count(), countIf(isValidJSON(json.__source)) FROM (SELECT * FROM generateRandom('json JSON(with_source=1)', 42, 10, 3) LIMIT 100);
SELECT count(), countIf(arrayAll(s -> isValidJSON(s), arr.__source)) FROM (SELECT * FROM generateRandom('arr Array(JSON(with_source=1))', 42, 10, 3) LIMIT 100);
