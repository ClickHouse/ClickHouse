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
