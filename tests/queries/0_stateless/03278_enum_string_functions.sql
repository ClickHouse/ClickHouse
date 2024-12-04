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
SELECT simpleJSONHas(json, 'foo') FROM jsons;
SELECT simpleJSONHas(json, 'a') FROM jsons;
SELECT simpleJSONExtractUInt(json, 'a') FROM jsons;
SELECT simpleJSONExtractUInt(json, 'not exsits') FROM jsons;
SELECT simpleJSONExtractInt(json, 'a') FROM jsons;
SELECT simpleJSONExtractInt(json, 'not exsits') FROM jsons;
SELECT simpleJSONExtractFloat(json, 'a') FROM jsons;
SELECT simpleJSONExtractFloat(json, 'not exsits') FROM jsons;
SELECT simpleJSONExtractBool(json, 'a') FROM jsons;
SELECT simpleJSONExtractBool(json, 'not exsits') FROM jsons;
SELECT positionUTF8(json, 'a') FROM jsons;
SELECT positionCaseInsensitiveUTF8(json, 'A') FROM jsons;
SELECT positionCaseInsensitive(json, 'A') FROM jsons;
