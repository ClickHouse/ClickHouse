-- { echoOn }

DROP TABLE IF EXISTS test_enum_string_functions;
CREATE TABLE test_enum_string_functions(e Enum('a'=1, 'b'=2)) ENGINE=TinyLog;
INSERT INTO test_enum_string_functions VALUES ('a');
SELECT * from test_enum_string_functions WHERE e LIKE '%abc%';
SELECT * from test_enum_string_functions WHERE e NOT LIKE '%abc%';
SELECT * from test_enum_string_functions WHERE e iLike '%a%';
SELECT position(e, 'a') FROM test_enum_string_functions;
SELECT match(e, 'a') FROM test_enum_string_functions;
SELECT locate('a', e) FROM test_enum_string_functions;
SELECT countSubstrings(e, 'a') FROM test_enum_string_functions;
SELECT countSubstringsCaseInsensitive(e, 'a') FROM test_enum_string_functions;
SELECT countSubstringsCaseInsensitiveUTF8(e, 'a') FROM test_enum_string_functions;
SELECT hasToken(e, 'a') FROM test_enum_string_functions;
SELECT hasTokenOrNull(e, 'a') FROM test_enum_string_functions;

DROP TABLE IF EXISTS jsons;
CREATE TABLE jsons
(
    `json` Enum('a', '{"a":1}')
)
ENGINE = Memory;
INSERT INTO jsons VALUES ('{"a":1}');
INSERT INTO jsons VALUES ('a');
SELECT simpleJSONHas(json, 'foo') as res FROM jsons order by res;
SELECT simpleJSONHas(json, 'a') as res FROM jsons order by res;
SELECT simpleJSONExtractUInt(json, 'a') as res FROM jsons order by res;
SELECT simpleJSONExtractUInt(json, 'not exsits') as res FROM jsons order by res;
SELECT simpleJSONExtractInt(json, 'a') as res FROM jsons order by res;
SELECT simpleJSONExtractInt(json, 'not exsits') as res FROM jsons order by res;
SELECT simpleJSONExtractFloat(json, 'a') as res FROM jsons order by res;
SELECT simpleJSONExtractFloat(json, 'not exsits') as res FROM jsons order by res;
SELECT simpleJSONExtractBool(json, 'a') as res FROM jsons order by res;
SELECT simpleJSONExtractBool(json, 'not exsits') as res FROM jsons order by res;
SELECT positionUTF8(json, 'a') as res FROM jsons order by res;
SELECT positionCaseInsensitiveUTF8(json, 'A') as res FROM jsons order by res;
SELECT positionCaseInsensitive(json, 'A') as res FROM jsons order by res;

SELECT materialize(CAST('a', 'Enum(\'a\' = 1)')) LIKE randomString(0) from numbers(10);
SELECT CAST('a', 'Enum(\'a\' = 1)') LIKE randomString(0); -- {serverError ILLEGAL_COLUMN}

SELECT materialize(CAST('a', 'Enum16(\'a\' = 1)')) LIKE randomString(0) from numbers(10);
SELECT CAST('a', 'Enum16(\'a\' = 1)') LIKE randomString(0); -- {serverError ILLEGAL_COLUMN}

SELECT CAST('a', 'Enum(\'a\' = 1)') LIKE 'a';
SELECT materialize(CAST('a', 'Enum(\'a\' = 1)')) LIKE 'a' from numbers(10);
