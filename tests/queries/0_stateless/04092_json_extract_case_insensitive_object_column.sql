SET allow_experimental_json_type = 1;

-- Case-insensitive extraction should find keys regardless of case
SELECT 'string lower', JSONExtractStringCaseInsensitive('{"Key": "value", "other": "x"}'::JSON, 'key');
SELECT 'string upper', JSONExtractStringCaseInsensitive('{"Key": "value", "other": "x"}'::JSON, 'KEY');
SELECT 'int nested', JSONExtractIntCaseInsensitive('{"Nested": {"InnerKey": 42}, "z": 0}'::JSON, 'nested', 'innerkey');
SELECT 'raw', JSONExtractRawCaseInsensitive('{"Key": "value", "other": "x"}'::JSON, 'key');

-- Case-sensitive extraction with exact case should still work
SELECT 'exact match', JSONExtractString('{"Key": "value"}'::JSON, 'Key');

-- Case-sensitive extraction with wrong case should return empty
SELECT 'wrong case', JSONExtractString('{"Key": "value"}'::JSON, 'key');

-- Multiple keys differing only in case: the result is non-deterministic,
-- but must be one of the matching values, not an unrelated key.
SELECT 'multi-key match',
    JSONExtractStringCaseInsensitive('{"Name": "alice", "NAME": "bob", "name": "charlie", "age": "30"}'::JSON, 'name')
    IN ('alice', 'bob', 'charlie');
SELECT 'multi-key int match',
    JSONExtractIntCaseInsensitive('{"Val": 1, "VAL": 2, "other": 99}'::JSON, 'val')
    IN (1, 2);

-- Mixed casing across rows: each row stores the key with a different case.
-- Case-insensitive extraction must resolve per row so no row gets a default value.
DROP TABLE IF EXISTS t_04092_mixed_case;
CREATE TABLE t_04092_mixed_case (id UInt32, j JSON) ENGINE = Memory;
INSERT INTO t_04092_mixed_case VALUES
    (1, '{"Name": "alice"}'),
    (2, '{"NAME": "bob"}'),
    (3, '{"name": "charlie"}'),
    (4, '{"other": "x"}');
SELECT 'mixed rows string', id, JSONExtractStringCaseInsensitive(j, 'name') FROM t_04092_mixed_case ORDER BY id;
SELECT 'mixed rows raw',    id, JSONExtractRawCaseInsensitive(j, 'name')    FROM t_04092_mixed_case ORDER BY id;

DROP TABLE IF EXISTS t_04092_mixed_case_int;
CREATE TABLE t_04092_mixed_case_int (id UInt32, j JSON) ENGINE = Memory;
INSERT INTO t_04092_mixed_case_int VALUES
    (1, '{"Val": 11}'),
    (2, '{"VAL": 22}'),
    (3, '{"val": 33}'),
    (4, '{"other": 99}');
SELECT 'mixed rows int', id, JSONExtractIntCaseInsensitive(j, 'val') FROM t_04092_mixed_case_int ORDER BY id;

DROP TABLE t_04092_mixed_case;
DROP TABLE t_04092_mixed_case_int;

-- Extracting a whole sub-object by a differently-cased key. The leaf is stored as a dotted
-- path (e.g. `Nested.InnerKey`), so the resolver must match the requested key against the
-- path prefix, mirroring case-sensitive sub-object extraction.
SELECT 'nested object raw',  JSONExtractRawCaseInsensitive('{"Nested": {"InnerKey": 42}}'::JSON, 'nested');
SELECT 'nested object raw exact', JSONExtractRaw('{"Nested": {"InnerKey": 42}}'::JSON, 'Nested');
SELECT 'nested object deep raw', JSONExtractRawCaseInsensitive('{"Nested": {"Inner": {"Deep": 7}}}'::JSON, 'nested', 'inner');

-- A typed path must not shadow a differently-cased key that carries a real value at a given row.
-- With a typed `Name` and max_dynamic_paths=0, a row's lowercase `name` lands in shared data;
-- case-insensitive lookup of `NAME` must return the real shared value (row 1), prefer the typed
-- value when it is actually set (row 2), and fall back to the typed default only when no casing
-- carries a value at that row (row 3).
DROP TABLE IF EXISTS t_04092_typed_shadow;
CREATE TABLE t_04092_typed_shadow (id UInt32, j JSON(Name String, max_dynamic_paths=0)) ENGINE = Memory;
INSERT INTO t_04092_typed_shadow VALUES
    (1, '{"name": "alice"}'),
    (2, '{"Name": "bob"}'),
    (3, '{"other": "x"}');
SELECT 'typed shadow string', id, JSONExtractStringCaseInsensitive(j, 'NAME') FROM t_04092_typed_shadow ORDER BY id;
SELECT 'typed shadow raw',    id, JSONExtractRawCaseInsensitive(j, 'NAME')    FROM t_04092_typed_shadow ORDER BY id;
DROP TABLE t_04092_typed_shadow;

-- Same shadowing for a typed numeric path: the real lowercase value must win over the typed default.
SELECT 'typed shadow int', JSONExtractIntCaseInsensitive('{"count": 5}'::JSON(Count Int64, max_dynamic_paths=0), 'COUNT');

-- The JSON type does not store `null` values: a path holding JSON `null` is dropped at parse time
-- and is indistinguishable from an absent key (the subcolumn returns NULL for both). So extracting
-- such a key returns the default, identically for the case-sensitive and case-insensitive variants.
SELECT 'json drops null', '{"Key": null}'::JSON;
SELECT 'null raw cs', JSONExtractRaw('{"Key": null}'::JSON, 'Key');
SELECT 'null raw ci', JSONExtractRawCaseInsensitive('{"Key": null}'::JSON, 'key');
-- On String input the raw text is preserved, so `null` is returned as-is.
SELECT 'null raw string input', JSONExtractRaw('{"Key": null}', 'Key');

-- Same per row: a stored value, a JSON `null`, and a missing key. The `null` and missing rows
-- must both return the default, matching the case-sensitive variant on the same data.
DROP TABLE IF EXISTS t_04092_null;
CREATE TABLE t_04092_null (id UInt32, j JSON) ENGINE = Memory;
INSERT INTO t_04092_null VALUES
    (1, '{"Key": "value"}'),
    (2, '{"Key": null}'),
    (3, '{}');
SELECT 'null rows cs', id, JSONExtractRaw(j, 'Key'), JSONExtractString(j, 'Key') FROM t_04092_null ORDER BY id;
SELECT 'null rows ci', id, JSONExtractRawCaseInsensitive(j, 'key'), JSONExtractStringCaseInsensitive(j, 'key') FROM t_04092_null ORDER BY id;
DROP TABLE t_04092_null;

-- Multiple casings across rows where one row holds a JSON `null`: the per-row resolver must
-- treat the `null` row as absent and return the default, not pick up another row's value.
DROP TABLE IF EXISTS t_04092_null_mixed;
CREATE TABLE t_04092_null_mixed (id UInt32, j JSON) ENGINE = Memory;
INSERT INTO t_04092_null_mixed VALUES
    (1, '{"Name": "alice"}'),
    (2, '{"name": null}'),
    (3, '{"NAME": "bob"}');
SELECT 'null mixed rows', id, JSONExtractRawCaseInsensitive(j, 'name'), JSONExtractStringCaseInsensitive(j, 'name') FROM t_04092_null_mixed ORDER BY id;
DROP TABLE t_04092_null_mixed;

-- The root form without any path argument must extract the whole JSON value instead of
-- silently returning the default, matching what the same call returns on the JSON string.
SELECT 'root raw', JSONExtractRaw('{"a": 1}'::JSON), JSONExtractRawCaseInsensitive('{"a": 1}'::JSON);
SELECT 'root extract', JSONExtract('{"a":"hello","b":[1]}'::JSON, 'Tuple(String, Array(UInt8))');
SELECT 'root scalars', JSONLength('{"a":1,"B":2}'::JSON), JSONType('{"a":1}'::JSON), isValidJSON('{"a":1}'::JSON), JSONHas('{"a":1}'::JSON);

-- Same for a materialized column, per row, including an empty object.
DROP TABLE IF EXISTS t_04092_root;
CREATE TABLE t_04092_root (id UInt32, j JSON) ENGINE = Memory;
INSERT INTO t_04092_root VALUES
    (1, '{"Key": "value"}'),
    (2, '{}'),
    (3, '{"n": {"x": 1}}');
SELECT 'root rows', id, JSONExtractRaw(j), JSONExtractRawCaseInsensitive(j), JSONLength(j) FROM t_04092_root ORDER BY id;
DROP TABLE t_04092_root;

-- An empty string is a legal JSON key. Extracting it from a `JSON` column must read the stored
-- empty key instead of being mistaken for the root form, matching the same call on a JSON string.
SELECT 'empty key cs', JSONExtractString('{"": "empty key"}'::JSON, ''), JSONExtractString('{"": "empty key"}', '');
SELECT 'empty key ci', JSONExtractStringCaseInsensitive('{"": "empty key"}'::JSON, ''), JSONExtractStringCaseInsensitive('{"": "empty key"}', '');
SELECT 'empty key raw', JSONExtractRaw('{"": "empty key", "a": 1}'::JSON, ''), JSONHas('{"": "empty key"}'::JSON, '');
-- An empty key is also legal in the middle of a path, on either side of a non-empty one.
SELECT 'empty key nested', JSONExtractInt('{"": {"b": 1}}'::JSON, '', 'b'), JSONExtractInt('{"": {"b": 1}}', '', 'b');
SELECT 'empty key nested ci', JSONExtractIntCaseInsensitive('{"": {"B": 1}}'::JSON, '', 'b'), JSONExtractIntCaseInsensitive('{"": {"B": 1}}', '', 'b');
SELECT 'empty key trailing', JSONExtractInt('{"a": {"": 1}}'::JSON, 'a', ''), JSONExtractInt('{"a": {"": 1}}', 'a', '');
-- A missing empty key still returns the default.
SELECT 'empty key missing', JSONExtractString('{"a": 1}'::JSON, ''), JSONHas('{"a": 1}'::JSON, '');

-- Same per row, so the empty key goes through the shared-data and per-row resolution paths too.
DROP TABLE IF EXISTS t_04092_empty_key;
CREATE TABLE t_04092_empty_key (id UInt32, j JSON) ENGINE = Memory;
INSERT INTO t_04092_empty_key VALUES
    (1, '{"": "empty"}'),
    (2, '{"a": 1}'),
    (3, '{"": "other"}');
SELECT 'empty key rows', id, JSONExtractString(j, ''), JSONExtractStringCaseInsensitive(j, '') FROM t_04092_empty_key ORDER BY id;
DROP TABLE t_04092_empty_key;

-- The root form must serialize the row with the caller's JSON format settings. With
-- `json_type_escape_dots_in_keys` a dot inside a key is escaped in the stored path, and only a
-- serialization that sees the setting unescapes it back, so the extracted text must round-trip.
SET json_type_escape_dots_in_keys = 1;
SELECT 'escaped dots root', JSONExtractRaw('{"a.b": 42}'::JSON), JSONExtractRawCaseInsensitive('{"a.b": 42}'::JSON);
SELECT 'escaped dots root keys', JSONExtractKeys('{"a.b": 42}'::JSON), JSONLength('{"a.b": 42}'::JSON);
SET json_type_escape_dots_in_keys = 0;

-- Every call shape on a `JSON` column returns what the same call returns on the equivalent JSON
-- string. The functions that navigate the document instead of reading one value out of it
-- (`JSONLength` and `JSONType` at a path, and the structural extractors) used to return a silent
-- default or to fail with `ILLEGAL_TYPE_OF_ARGUMENT` on a `JSON` column.
SELECT 'nav length', JSONLength('{"a": {"b": 1, "d": [1, 2]}}'::JSON, 'a'), JSONLength('{"a": {"b": 1, "d": [1, 2]}}', 'a');
SELECT 'nav type object', JSONType('{"a": {"b": 1}}'::JSON, 'a'), JSONType('{"a": {"b": 1}}', 'a');
SELECT 'nav type nested', JSONType('{"a": {"d": [1, 2]}}'::JSON, 'a', 'd'), JSONType('{"a": {"d": [1, 2]}}', 'a', 'd');
SELECT 'nav keys', JSONExtractKeys('{"a": {"b": 1, "d": 2}}'::JSON, 'a'), JSONExtractKeys('{"a": {"b": 1, "d": 2}}', 'a');
SELECT 'nav keys and values', JSONExtractKeysAndValues('{"a": {"b": 1, "d": 2}}'::JSON, 'a', 'Int64'), JSONExtractKeysAndValues('{"a": {"b": 1, "d": 2}}', 'a', 'Int64');
SELECT 'nav keys and values raw', JSONExtractKeysAndValuesRaw('{"a": {"b": 1}}'::JSON, 'a'), JSONExtractKeysAndValuesRaw('{"a": {"b": 1}}', 'a');
SELECT 'nav array raw', JSONExtractArrayRaw('{"a": [1, 2]}'::JSON, 'a'), JSONExtractArrayRaw('{"a": [1, 2]}', 'a');
-- The case-insensitive structural variants match their keys on a `JSON` column too.
SELECT 'nav keys ci', JSONExtractKeysCaseInsensitive('{"Nested": {"b": 1}}'::JSON, 'NESTED'), JSONExtractKeysCaseInsensitive('{"Nested": {"b": 1}}', 'NESTED');
SELECT 'nav array raw ci', JSONExtractArrayRawCaseInsensitive('{"Arr": [1, 2]}'::JSON, 'ARR'), JSONExtractArrayRawCaseInsensitive('{"Arr": [1, 2]}', 'ARR');
SELECT 'nav keys and values raw ci', JSONExtractKeysAndValuesRawCaseInsensitive('{"Nested": {"b": 1}}'::JSON, 'NESTED'), JSONExtractKeysAndValuesRawCaseInsensitive('{"Nested": {"b": 1}}', 'NESTED');
SELECT 'nav keys and values ci', JSONExtractKeysAndValuesCaseInsensitive('{"Nested": {"b": 1}}'::JSON, 'NESTED', 'Int64'), JSONExtractKeysAndValuesCaseInsensitive('{"Nested": {"b": 1}}', 'NESTED', 'Int64');

-- An integer index addresses a member of an array or an object; a subcolumn name cannot express it,
-- so these call shapes read the JSON text of the row.
SELECT 'index array', JSONExtractInt('{"a": [10, 20]}'::JSON, 'a', 2), JSONExtractInt('{"a": [10, 20]}', 'a', 2);
SELECT 'index raw', JSONExtractRaw('{"a": [10, 20]}'::JSON, 'a', 1), JSONExtractRaw('{"a": [10, 20]}', 'a', 1);
SELECT 'index key', JSONKey('{"a": 1}'::JSON, 1), JSONKey('{"a": 1}', 1);

-- Path keys that are not constant are resolved per row, and are matched case-insensitively by the
-- case-insensitive variants.
DROP TABLE IF EXISTS t_04092_nonconst;
CREATE TABLE t_04092_nonconst (id UInt32, j JSON, s String, k String) ENGINE = Memory;
INSERT INTO t_04092_nonconst VALUES
    (1, '{"Name": "alice", "age": 30}', '{"Name": "alice", "age": 30}', 'Name'),
    (2, '{"name": "bob", "age": 40}', '{"name": "bob", "age": 40}', 'age'),
    (3, '{"other": 1}', '{"other": 1}', 'missing');
SELECT 'nonconst key', id,
    JSONExtractRaw(j, k) = JSONExtractRaw(s, k),
    JSONHas(j, k) = JSONHas(s, k),
    JSONExtractRawCaseInsensitive(j, upper(k)) = JSONExtractRawCaseInsensitive(s, upper(k))
FROM t_04092_nonconst ORDER BY id;
DROP TABLE t_04092_nonconst;
