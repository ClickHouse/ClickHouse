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

SELECT 'prewhere_group_by';
SELECT count() FROM test_json_typed_object PREWHERE json['a']['b'] = 42 SETTINGS optimize_functions_to_subcolumns = 1;
SELECT json['a']['b'] AS v, count() FROM test_json_typed_object GROUP BY v SETTINGS optimize_functions_to_subcolumns = 1;

-- A typed path of type `JSON` without its own typed paths: the value was read as NULL.
DROP TABLE IF EXISTS test_json_untyped_object;
CREATE TABLE test_json_untyped_object (
    json JSON(a JSON)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_json_untyped_object VALUES ('{"a": {"b": 42}}');

SELECT 'untyped_object_path_chained';
SELECT json['a']['b'] FROM test_json_untyped_object SETTINGS optimize_functions_to_subcolumns = 1;
SELECT json['a']['b'] FROM test_json_untyped_object SETTINGS optimize_functions_to_subcolumns = 0;

-- A dotted typed path deeper in the chain: `a.b` is a typed path, `a` is not.
DROP TABLE IF EXISTS test_json_deep_typed_prefix;
CREATE TABLE test_json_deep_typed_prefix (
    json JSON(a.b JSON(c UInt32))
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_json_deep_typed_prefix VALUES ('{"a": {"b": {"c": 3}}}');

SELECT 'deep_typed_prefix_chained';
SELECT json['a']['b']['c'] FROM test_json_deep_typed_prefix SETTINGS optimize_functions_to_subcolumns = 1;
SELECT json['a']['b']['c'] FROM test_json_deep_typed_prefix SETTINGS optimize_functions_to_subcolumns = 0;

-- Dotted typed paths (where prefix 'c' is not a typed path itself) should continue to work
DROP TABLE IF EXISTS test_json_dotted_typed;
CREATE TABLE test_json_dotted_typed (
    json JSON(c.d UInt32)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_json_dotted_typed VALUES ('{"c": {"d": 10}}');

SELECT 'dotted_typed_path';
SELECT json['c']['d'], json.c.d FROM test_json_dotted_typed SETTINGS optimize_functions_to_subcolumns = 1;
SELECT json['c']['d'], json.c.d FROM test_json_dotted_typed SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS test_json_typed_object;
DROP TABLE IF EXISTS test_json_untyped_object;
DROP TABLE IF EXISTS test_json_deep_typed_prefix;
DROP TABLE IF EXISTS test_json_dotted_typed;
