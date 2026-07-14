-- Test that nested subcolumn access on typed paths works without [] sugar.
-- E.g. json.a.b where a is a typed path of type Array(JSON).

SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_typed_json;

SELECT 'Case 1: basic nested subcolumn on Array(JSON) typed path';
SELECT '{"a" : [{"b" : 42}]}'::JSON(a Array(JSON)) AS json, json.a.b;

SELECT 'Case 2: table with typed Array(JSON), access nested subcolumn without []';
CREATE TABLE test_typed_json (json JSON(a Array(JSON))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a":[{"b":1,"c":"hello"},{"b":2,"c":"world"}]}');
INSERT INTO test_typed_json VALUES ('{"a":[{"b":3}]}');
INSERT INTO test_typed_json VALUES ('{"a":[]}');
INSERT INTO test_typed_json VALUES ('{}');

SELECT json.a.b FROM test_typed_json ORDER BY toString(json);
SELECT json.a.c FROM test_typed_json ORDER BY toString(json);

DROP TABLE test_typed_json;

SELECT 'Case 3: deeper typed path prefix (a.b is typed, access a.b.c)';
CREATE TABLE test_typed_json (json JSON(a.b Array(JSON))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a":{"b":[{"c":10},{"c":20}]}}');

SELECT json.a.b.c FROM test_typed_json;

DROP TABLE test_typed_json;

SELECT 'Case 4: nested subcolumn with type hint downstream';
-- json.a.b.:`Int64` where a is typed Array(JSON) —
-- the .:`Int64` should be preserved and resolved inside the nested JSON.
CREATE TABLE test_typed_json (json JSON(a Array(JSON))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a":[{"b":1},{"b":2}]}');

SELECT json.a.b.:Int64 FROM test_typed_json;

DROP TABLE test_typed_json;

SELECT 'Case 5: nested subcolumn on typed JSON (not Array)';
CREATE TABLE test_typed_json (json JSON(a JSON)) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a":{"b":42,"c":"hello"}}');

SELECT json.a.b FROM test_typed_json;
SELECT json.a.c FROM test_typed_json;

DROP TABLE test_typed_json;

SELECT 'Case 6: MergeTree with nested subcolumn on typed path';
CREATE TABLE test_typed_json (id UInt64, json JSON(a Array(JSON)))
ENGINE = MergeTree ORDER BY id;
INSERT INTO test_typed_json VALUES (1, '{"a":[{"x":1},{"x":2}]}');
INSERT INTO test_typed_json VALUES (2, '{"a":[{"x":3}]}');

SELECT id, json.a.x FROM test_typed_json ORDER BY id;

DROP TABLE test_typed_json;

SELECT 'Case 7: typed path exact match takes priority over prefix match';
-- Both a (Array(JSON)) and a.b (Int64) are typed paths.
-- json.a.b should match the exact typed path a.b, not prefix a.
CREATE TABLE test_typed_json (json JSON(a Array(JSON), a.b Int64)) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a":[{"x":1}], "a.b":42}');

SELECT json.a.b FROM test_typed_json;

DROP TABLE test_typed_json;

SELECT 'Case 8: nested subcolumn compared to [] sugar';
-- json.a.b and json.a[].b should produce the same result.
CREATE TABLE test_typed_json (json JSON(a Array(JSON))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a":[{"b":1},{"b":2}]}');
INSERT INTO test_typed_json VALUES ('{"a":[{"b":3}]}');

SELECT json.a.b AS without_sugar, json.a[].b AS with_sugar FROM test_typed_json ORDER BY toString(json);

DROP TABLE test_typed_json;
