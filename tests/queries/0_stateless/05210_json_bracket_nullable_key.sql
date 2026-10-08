-- Bracket syntax `json['key']` with a nullable key: `arrayElement` accepts a constant `Nullable(String)`
-- key and a NULL key for `Array` and `Map`, and the `JSON` overload must keep that contract.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_json_nullable_key;
CREATE TABLE t_json_nullable_key (json JSON(a UInt32, arr Array(UInt32))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_nullable_key VALUES ('{"a": 1, "arr": [1, 2], "b": "x"}'), ('{"a": 2, "arr": [], "b": 5}');

SELECT 'nullable_string_key';
-- The result is promoted the way the other source types promote it for a nullable index: a typed path
-- that can be wrapped becomes `Nullable`, a `Dynamic` path carries NULL itself, an `Array` path cannot.
SELECT
    json[CAST('a', 'Nullable(String)')] AS a, toTypeName(a),
    json[CAST('b', 'Nullable(String)')] AS b, toTypeName(b),
    json[CAST('arr', 'Nullable(String)')] AS arr, toTypeName(arr)
FROM t_json_nullable_key;

SELECT 'nullable_string_key_values_match_plain_key';
SELECT json[CAST('a', 'Nullable(String)')] = json['a'], json[CAST('b', 'Nullable(String)')] = json['b'], json[CAST('arr', 'Nullable(String)')] = json['arr']
FROM t_json_nullable_key;

SELECT 'null_key';
SELECT json[NULL] AS v, toTypeName(v) FROM t_json_nullable_key;
SELECT json[CAST(NULL, 'Nullable(String)')] AS v, toTypeName(v) FROM t_json_nullable_key;

SELECT 'nullable_json_and_nullable_key';
SELECT CAST('{"a": 1}', 'Nullable(JSON(a UInt32))') AS json, json[CAST('a', 'Nullable(String)')] AS v, toTypeName(v);
SELECT CAST(NULL, 'Nullable(JSON(a UInt32))') AS json, json[CAST('a', 'Nullable(String)')] AS v, toTypeName(v);

SELECT 'non_constant_key_is_rejected';
SELECT json[materialize(CAST('a', 'Nullable(String)'))] FROM t_json_nullable_key; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE t_json_nullable_key;

SELECT 'lowcardinality_typed_path';
DROP TABLE IF EXISTS t_json_nullable_key_lc;
CREATE TABLE t_json_nullable_key_lc (json JSON(s LowCardinality(String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_nullable_key_lc VALUES ('{"s": "x"}'), ('{"s": "y"}');

-- A `LowCardinality(T)` path is promoted to `LowCardinality(Nullable(T))`, where the `Nullable` lives in the
-- dictionary. `optimize_functions_to_subcolumns` casts the rewritten read to the declared type, which hides a
-- disagreement between the two, so pin the setting and keep the enabled arm as the control.
SELECT json[CAST('s', 'Nullable(String)')] AS s, toTypeName(s), s = json['s']
FROM t_json_nullable_key_lc ORDER BY s SETTINGS optimize_functions_to_subcolumns = 0;
SELECT json[CAST('s', 'Nullable(String)')] AS s, toTypeName(s), s = json['s']
FROM t_json_nullable_key_lc ORDER BY s SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_json_nullable_key_lc;

SELECT 'lowcardinality_typed_path_constant_source';
-- An all-constant call is dispatched with the `LowCardinality` result wrapper already stripped, so the
-- extracted path has to give up its own, but only its own: a wrapper nested in the path's type stays.
SELECT
    CAST('{"s": "x"}', 'JSON(s LowCardinality(String))')['s'] AS plain_key, toTypeName(plain_key),
    CAST('{"s": "x"}', 'JSON(s LowCardinality(String))')[CAST('s', 'Nullable(String)')] AS nullable_key, toTypeName(nullable_key),
    CAST('{"arr": ["x"]}', 'JSON(arr Array(LowCardinality(String)))')['arr'] AS nested_lc, toTypeName(nested_lc)
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'lowcardinality_typed_path_nullable_json';
-- The outer null map is folded into the dictionary, so the promotion the nullable key asks for is already
-- there and must not be applied a second time.
SELECT if(number = 0, CAST('{"s": "x"}', 'Nullable(JSON(s LowCardinality(String)))'), NULL) AS json,
    json[CAST('s', 'Nullable(String)')] AS s, toTypeName(s)
FROM numbers(2) SETTINGS optimize_functions_to_subcolumns = 0;
