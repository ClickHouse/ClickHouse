-- The source subcolumn in various contexts.

SELECT 'subcolumn of the source';
SELECT json.__source.size FROM (SELECT materialize('{"a" : 42}')::JSON(with_source=1) AS json);

DROP TABLE IF EXISTS t_json_source_misc;
CREATE TABLE t_json_source_misc (json JSON(with_source=1)) ENGINE = MergeTree ORDER BY tuple();
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
