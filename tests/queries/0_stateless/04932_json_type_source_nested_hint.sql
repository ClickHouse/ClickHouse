-- A type hint can be a JSON type with the source itself. Only the top level column gets the original
-- JSON text, nested ones create it from their part of the object.

SELECT 'typed path with source';
DROP TABLE IF EXISTS t_json_source_nested;
CREATE TABLE t_json_source_nested (json JSON(with_source=1, a JSON(with_source=1))) ENGINE = Memory;
INSERT INTO t_json_source_nested VALUES ('{"a" : {"b" : 42,  "c" : "Hello"},   "d" : 1}');
SELECT json.__source FROM t_json_source_nested;
SELECT json.a.__source FROM t_json_source_nested;
SELECT toTypeName(json.__source), toTypeName(json.a.__source) FROM t_json_source_nested;
SELECT json FROM t_json_source_nested;
DROP TABLE t_json_source_nested;

SELECT 'array of JSON with source';
DROP TABLE IF EXISTS t_json_source_nested_array;
CREATE TABLE t_json_source_nested_array (json JSON(with_source=1, a Array(JSON(with_source=1)))) ENGINE = Memory;
INSERT INTO t_json_source_nested_array VALUES ('{"a" : [{"b" : 42}, {"c" :  "Hello"}]}');
SELECT json.__source FROM t_json_source_nested_array;
SELECT arrayMap(x -> getSubcolumn(x, '__source'), json.a) FROM t_json_source_nested_array;
DROP TABLE t_json_source_nested_array;

SELECT 'no original text at any level';
SELECT json.__source, json.a.__source FROM (SELECT JSONExtract('{"a" : {"b" : 42},  "d" : 1}', 'JSON(with_source=1, a JSON(with_source=1))') AS json);

SELECT 'null with input_format_null_as_default';
SELECT json.__source FROM format(JSONEachRow, 'json JSON(with_source=1)', '{"json" : null}') SETTINGS input_format_null_as_default = 1;
