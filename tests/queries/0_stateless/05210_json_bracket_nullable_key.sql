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
