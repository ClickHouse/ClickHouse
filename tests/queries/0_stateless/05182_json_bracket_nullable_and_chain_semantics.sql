-- Bracket syntax `json['key']` on a `Nullable(JSON)` carrier, and the semantics of chained
-- bracket access over a path that holds a scalar in some rows.

SET enable_analyzer = 1;

SELECT 'nullable_json_untyped';
DROP TABLE IF EXISTS t_nullable_json;
CREATE TABLE t_nullable_json (json Nullable(JSON)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nullable_json VALUES ('{"a": 1, "c": {"d": 5}}'), (NULL), ('{"a": "x"}');

-- The bracket syntax must agree with the dot syntax and with `tupleElement`, both in value and in type.
SELECT
    json['a'],
    toTypeName(json['a']),
    json['a'] IS NULL AS is_null,
    json.a AS dot,
    tupleElement(json, 'a') AS tuple_element
FROM t_nullable_json;

SELECT toTypeName(json['a']) = toTypeName(json.a) AS same_type_as_dot FROM t_nullable_json LIMIT 1;

SELECT 'nullable_json_chain';
SELECT json['c']['d'] FROM t_nullable_json;

SELECT 'nullable_json_typed';
DROP TABLE IF EXISTS t_nullable_json_typed;
CREATE TABLE t_nullable_json_typed (json Nullable(JSON(a UInt32, arr Array(UInt32))))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nullable_json_typed VALUES ('{"a": 1, "arr": [1, 2]}'), (NULL);

-- A typed path that can be wrapped in `Nullable` carries the outer NULL, a typed `Array` path has no
-- NULL representation and is default-filled -- exactly like reading the subcolumn directly.
SELECT json['a'], toTypeName(json['a']), json.a, json['arr'], toTypeName(json['arr']), json.arr
FROM t_nullable_json_typed;

SELECT 'nullable_json_const';
SELECT CAST('{"a": 1}', 'Nullable(JSON)') AS json, json['a'], toTypeName(json['a']);
SELECT CAST(NULL, 'Nullable(JSON)') AS json, json['a'];

-- `arrayElementOrNull` is not defined for JSON, and a `Nullable(JSON)` carrier is no exception.
SELECT arrayElementOrNull(CAST('{"a": 1}', 'Nullable(JSON)'), 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'chain_over_scalar_path';
DROP TABLE IF EXISTS t_mixed_json;
CREATE TABLE t_mixed_json (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_mixed_json VALUES ('{"c": 42}'), ('{"c": {"d": 1}}');

-- `json['c']['d']` on a path that is a scalar in one row and an object in another.
-- With `optimize_functions_to_subcolumns` the chain is flattened to the JSON path `c.d`, which is the
-- same thing `json.c.d` reads: a row whose `c` is a scalar simply has no `c.d`, so the result is NULL.
SELECT json['c']['d'] FROM t_mixed_json SETTINGS optimize_functions_to_subcolumns = 1;
SELECT json.c.d FROM t_mixed_json;

-- Without the flattening the outer `arrayElement` is applied to the `Dynamic` result of `json['c']`,
-- so the scalar row goes through the `Dynamic` type mismatch rules instead: an exception with the
-- default `dynamic_throw_on_type_mismatch = 1`, and the same NULL with it disabled.
SELECT json['c']['d'] FROM t_mixed_json
SETTINGS optimize_functions_to_subcolumns = 0, dynamic_throw_on_type_mismatch = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT json['c']['d'] FROM t_mixed_json
SETTINGS optimize_functions_to_subcolumns = 0, dynamic_throw_on_type_mismatch = 0;

DROP TABLE t_nullable_json;
DROP TABLE t_nullable_json_typed;
DROP TABLE t_mixed_json;
