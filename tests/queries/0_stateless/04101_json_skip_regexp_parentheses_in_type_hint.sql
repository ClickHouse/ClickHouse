-- Tags: no-fasttest
-- Tests that `removeJSONTypeParameters` correctly skips single-quoted strings
-- so that unbalanced parentheses inside SKIP REGEXP patterns don't break
-- nesting depth tracking in `removeJSONTypeParameters`.
-- Without the fix, an unbalanced `(` inside a single-quoted string would
-- increment the depth counter, causing the function to consume the real
-- closing `)` of `JSON(...)` and produce a malformed type name.

SET allow_experimental_json_type = 1;

DROP TABLE IF EXISTS test_typed_json;

-- SKIP REGEXP with a single unbalanced open parenthesis.
-- Without skipping single-quoted strings, `removeJSONTypeParameters` sees `\(`
-- as a real parenthesis, increments depth, and then eats the closing `)` of Array().
CREATE TABLE test_typed_json (json JSON(a1 Array(JSON(SKIP REGEXP '\(')))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a1":[{"b":1}], "a2":[{"b":1}]}');

SELECT json.a1[].b AS typed, json.a2[].b AS dynamic FROM test_typed_json;

DROP TABLE test_typed_json;

-- SKIP REGEXP with a single unbalanced close parenthesis.
CREATE TABLE test_typed_json (json JSON(a1 Array(JSON(SKIP REGEXP '\)')))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a1":[{"b":2}], "a2":[{"b":2}]}');

SELECT json.a1[].b AS typed, json.a2[].b AS dynamic FROM test_typed_json;

DROP TABLE test_typed_json;

-- SKIP REGEXP with unbalanced parenthesis combined with a typed path.
CREATE TABLE test_typed_json (json JSON(a1 Array(JSON(x UInt64, SKIP REGEXP 'prefix\(')))) ENGINE = Memory;
INSERT INTO test_typed_json VALUES ('{"a1":[{"x":42}], "a2":[{"x":42}]}');

SELECT json.a1[].x AS typed, json.a2[].x AS dynamic FROM test_typed_json;

DROP TABLE test_typed_json;
