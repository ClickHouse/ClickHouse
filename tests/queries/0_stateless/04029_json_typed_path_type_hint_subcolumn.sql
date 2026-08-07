DROP TABLE IF EXISTS test_typed_json;

SELECT 'Case 1';
-- Typed vs dynamic Array(JSON) — compare [] sugar results.
-- Insert identical data into typed path "a1" and dynamic path "a2",
-- then verify json.a1[].subpath == json.a2[].subpath.
CREATE TABLE test_typed_json (json JSON(a1 Array(JSON))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a1":[{"b":1,"c":"hello"},{"b":2,"c":"world"}], "a2":[{"b":1,"c":"hello"},{"b":2,"c":"world"}]}');
INSERT INTO test_typed_json VALUES ('{"a1":[{"b":3,"d":true}], "a2":[{"b":3,"d":true}]}');
INSERT INTO test_typed_json VALUES ('{"a1":[], "a2":[]}');
INSERT INTO test_typed_json VALUES ('{}');

SELECT json.a1[].b AS typed, json.a2[].b AS dynamic FROM test_typed_json ORDER BY toString(json);
SELECT json.a1[].c AS typed, json.a2[].c AS dynamic FROM test_typed_json ORDER BY toString(json);
SELECT json.a1[].d AS typed, json.a2[].d AS dynamic FROM test_typed_json ORDER BY toString(json);
-- With explicit type hint after []:
SELECT json.a1[].b.:Int64 AS typed, json.a2[].b.:Int64 AS dynamic FROM test_typed_json ORDER BY toString(json);

DROP TABLE test_typed_json;

SELECT 'Case 2';
-- Deeper path nesting — typed vs dynamic.
CREATE TABLE test_typed_json (json JSON(a1.b Array(JSON))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a1":{"b":[{"x":10},{"x":20}]}, "a2":{"b":[{"x":10},{"x":20}]}}');

SELECT json.a1.b[].x AS typed, json.a2.b[].x AS dynamic FROM test_typed_json;

DROP TABLE test_typed_json;

SELECT 'Case 3';
-- Multiple typed Array(JSON) paths — typed vs dynamic.
CREATE TABLE test_typed_json (json JSON(a1 Array(JSON), b1 Array(JSON))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a1":[{"x":1}],"b1":[{"y":2}],"a2":[{"x":1}],"b2":[{"y":2}]}');

SELECT json.a1[].x AS typed, json.a2[].x AS dynamic FROM test_typed_json;
SELECT json.b1[].y AS typed, json.b2[].y AS dynamic FROM test_typed_json;

DROP TABLE test_typed_json;

SELECT 'Case 4';
-- MergeTree engine — typed vs dynamic.
CREATE TABLE test_typed_json (id UInt64, json JSON(a1 Array(JSON)))
ENGINE = MergeTree ORDER BY id;
INSERT INTO test_typed_json VALUES (1, '{"a1":[{"name":"a","val":1},{"name":"b","val":2}], "a2":[{"name":"a","val":1},{"name":"b","val":2}]}');
INSERT INTO test_typed_json VALUES (2, '{"a1":[{"name":"c","val":3}], "a2":[{"name":"c","val":3}]}');

SELECT id, json.a1[].name AS typed, json.a2[].name AS dynamic FROM test_typed_json ORDER BY id;
SELECT id, json.a1[].val AS typed, json.a2[].val AS dynamic FROM test_typed_json ORDER BY id;

DROP TABLE test_typed_json;

SELECT 'Case 5';
-- Empty arrays and missing paths.
CREATE TABLE test_typed_json (json JSON(a1 Array(JSON))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a1":[], "a2":[]}');
INSERT INTO test_typed_json VALUES ('{}');
INSERT INTO test_typed_json VALUES ('{"a1":[{"x":1}], "a2":[{"x":1}]}');

SELECT json.a1[].x AS typed, json.a2[].x AS dynamic FROM test_typed_json ORDER BY json;

DROP TABLE test_typed_json;

SELECT 'Case 6';
-- Explicit .:`Type` on typed path (general case, not [] sugar).
CREATE TABLE test_typed_json (json JSON(a1 Array(JSON), c Int64)) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a1":[{"x":1}], "a2":[{"x":1}], "c":42}');

-- Explicit type hint matching the typed path's type:
SELECT json.a1.:`Array(JSON)`.x AS typed, json.a2.:`Array(JSON)`.x AS dynamic FROM test_typed_json;
-- Type hint on non-Array typed path:
SELECT json.c.:`Int64` FROM test_typed_json;

DROP TABLE test_typed_json;

SELECT 'Case 7';
-- Sub-object access combined with [] on typed path.
CREATE TABLE test_typed_json (json JSON(a1 Array(JSON))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a1":[{"nested":{"x":1,"y":2}},{"nested":{"x":3,"y":4}}], "a2":[{"nested":{"x":1,"y":2}},{"nested":{"x":3,"y":4}}]}');

SELECT json.a1[].nested.x AS typed, json.a2[].nested.x AS dynamic FROM test_typed_json;
SELECT json.a1[].^nested AS typed, json.a2[].^nested AS dynamic FROM test_typed_json;

DROP TABLE test_typed_json;

SELECT 'Case 8';
-- Mismatched type hint — nesting mismatch, should NOT strip.
CREATE TABLE test_typed_json (json JSON(a Array(JSON))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a":[{"x":1}]}');

-- [][] on single Array(JSON) — nesting mismatch, expect [NULL]
SELECT json.a[][].x FROM test_typed_json;

DROP TABLE test_typed_json;

SELECT 'Case 9';
-- Completely wrong type hint on typed path — should NOT strip.
CREATE TABLE test_typed_json (json JSON(a Array(JSON))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a":[{"x":1}]}');

-- .:`String` on an Array(JSON) typed path — type mismatch:
SELECT json.a.:`String` FROM test_typed_json;

DROP TABLE test_typed_json;

SELECT 'Case 10';
-- Nested Array(Array(JSON)) — typed vs dynamic with [][].
CREATE TABLE test_typed_json (json JSON(a1 Array(Array(JSON)))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a1":[[{"x":1},{"x":2}],[{"x":3}]], "a2":[[{"x":1},{"x":2}],[{"x":3}]]}');
INSERT INTO test_typed_json VALUES ('{"a1":[[{"y":"hello"}]], "a2":[[{"y":"hello"}]]}');

SELECT json.a1[][].x AS typed, json.a2[][].x AS dynamic FROM test_typed_json ORDER BY toString(json);
SELECT json.a1[][].y AS typed, json.a2[][].y AS dynamic FROM test_typed_json ORDER BY toString(json);

DROP TABLE test_typed_json;

SELECT 'Case 11';
-- Typed Array(JSON) with explicit JSON parameters
-- including SKIP with round brackets in backticks.
CREATE TABLE test_typed_json (json JSON(a1 Array(JSON(max_dynamic_paths=16, max_dynamic_types=4, SKIP `some()path`)))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a1":[{"b":1,"c":"text"}], "a2":[{"b":1,"c":"text"}]}');
INSERT INTO test_typed_json VALUES ('{"a1":[{"b":2,"d":[1,2,3]}], "a2":[{"b":2,"d":[1,2,3]}]}');

SELECT json.a1[].b AS typed, json.a2[].b AS dynamic FROM test_typed_json ORDER BY toString(json);
SELECT json.a1[].c AS typed, json.a2[].c AS dynamic FROM test_typed_json ORDER BY toString(json);

DROP TABLE test_typed_json;

SELECT 'Case 12';
-- Typed Array(JSON) with typed subpaths inside JSON.
CREATE TABLE test_typed_json (json JSON(a1 Array(JSON(x UInt64, y Array(String))))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a1":[{"x":1,"y":["a","b"]},{"x":2,"y":["c"]}], "a2":[{"x":1,"y":["a","b"]},{"x":2,"y":["c"]}]}');

SELECT json.a1[].x AS typed, json.a2[].x AS dynamic FROM test_typed_json;
SELECT json.a1[].y AS typed, json.a2[].y AS dynamic FROM test_typed_json;

DROP TABLE test_typed_json;

SELECT 'Case 13';
-- Explicit .:`Array(JSON(...))` with full parameters on a typed path —
-- parameters differ from declared type but after stripping JSON params
-- both normalize to Array(JSON).
CREATE TABLE test_typed_json (json JSON(a1 Array(JSON(max_dynamic_paths=32)))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a1":[{"z":"val"}]}');

-- Explicit hint with different JSON parameters than the declared type:
SELECT json.a1.:`Array(JSON)`.z FROM test_typed_json;
SELECT json.a1.:`Array(JSON(max_dynamic_paths=100))`.z FROM test_typed_json;

DROP TABLE test_typed_json;

SELECT 'Case 14';
-- Nested Array(JSON) inside typed Array(JSON) — chained [] sugar.
-- Typed path is Array(JSON(inner Array(JSON))), so json.a1[].inner[].leaf
-- must strip the first [] for the typed outer Array(JSON), then resolve
-- inner[].leaf inside the nested JSON where inner is itself a typed Array(JSON).
CREATE TABLE test_typed_json (json JSON(a1 Array(JSON(inner Array(JSON))))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a1":[{"inner":[{"leaf":1},{"leaf":2}]},{"inner":[{"leaf":3}]}], "a2":[{"inner":[{"leaf":1},{"leaf":2}]},{"inner":[{"leaf":3}]}]}');

SELECT json.a1[].inner[].leaf AS typed, json.a2[].inner[].leaf AS dynamic FROM test_typed_json;

DROP TABLE test_typed_json;

SELECT 'Case 15';
-- JSON parameter with escaped backticks in back-quoted path.
-- Tests that `removeJSONTypeParameters` correctly handles doubled backticks
-- inside back-quoted strings (e.g. `some``()path`).
CREATE TABLE test_typed_json (json JSON(a1 Array(JSON(SKIP `some``()path`)))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a1":[{"b":1}], "a2":[{"b":1}]}');

SELECT json.a1[].b AS typed, json.a2[].b AS dynamic FROM test_typed_json;

DROP TABLE test_typed_json;
