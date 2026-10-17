-- Tags: no-fasttest
-- https://github.com/ClickHouse/ClickHouse/issues/119980
-- Chained json['a']['b'] bracket access must not flatten across typed paths with subcolumns.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_json_typed_object;
CREATE TABLE test_json_typed_object (
    json JSON(a JSON(b UInt32), m Map(String, UInt32))
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_json_typed_object VALUES ('{"a": {"b": 42}, "m": {"k": 7}}');

SELECT 'typed_object_path_chained';
SELECT json.a.b, json['a']['b'], json.m['k'], json['m']['k'] FROM test_json_typed_object SETTINGS optimize_functions_to_subcolumns = 1;
SELECT json.a.b, json['a']['b'], json.m['k'], json['m']['k'] FROM test_json_typed_object SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'where_filter';
SELECT count() FROM test_json_typed_object WHERE json['a']['b'] = 42 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM test_json_typed_object WHERE json['m']['k'] = 7 SETTINGS optimize_functions_to_subcolumns = 1;

-- Dotted typed paths (where prefix 'c' is not a typed path itself) should continue to work
DROP TABLE IF EXISTS test_json_dotted_typed;
CREATE TABLE test_json_dotted_typed (
    json JSON(c.d UInt32)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_json_dotted_typed VALUES ('{"c": {"d": 10}}');

SELECT 'dotted_typed_path';
SELECT json['c']['d'], json.c.d FROM test_json_dotted_typed SETTINGS optimize_functions_to_subcolumns = 1;
SELECT json['c']['d'], json.c.d FROM test_json_dotted_typed SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE test_json_typed_object;
DROP TABLE test_json_dotted_typed;
