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
