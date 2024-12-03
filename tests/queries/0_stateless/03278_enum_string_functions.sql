DROP TABLE IF EXISTS test_enum_string_functions;
CREATE TABLE test_enum_string_functions(e Enum('a'=1, 'b'=2)) ENGINE=TinyLog;
INSERT INTO test_enum_string_functions VALUES ('a');
-- like --
SELECT * from test_enum_string_functions WHERE e LIKE '%abc%';
-- not like --
SELECT * from test_enum_string_functions WHERE e NOT LIKE '%abc%';
-- ilike --
SELECT * from test_enum_string_functions WHERE e iLike '%a%';
-- position --
SELECT position(e, 'a') FROM test_enum_string_functions;
-- match --
SELECT match(e, 'a') FROM test_enum_string_functions;
-- locate --
SELECT locate('a', e) FROM test_enum_string_functions;
-- countsubstrings --
SELECT countSubstrings(e, 'a') FROM test_enum_string_functions;
-- countSburstringsCaseInsensitive --
SELECT countSubstringsCaseInsensitive(e, 'a') FROM test_enum_string_functions;
-- countSubstringsCaseInsensitiveUTF8 --
SELECT countSubstringsCaseInsensitiveUTF8(e, 'a') FROM test_enum_string_functions;
-- hasToken --
SELECT hasToken(e, 'a') FROM test_enum_string_functions;
-- hasTokenOrNull --
SELECT hasTokenOrNull(e, 'a') FROM test_enum_string_functions;

DROP TABLE jsons;
CREATE TABLE jsons
(
    `json` Enum('a', '{"a":1}')
)
ENGINE = Memory;
INSERT INTO jsons VALUES ('{"a":1}');
INSERT INTO jsons VALUES ('a');
-- simpleJSONHas --
SELECT simpleJSONHas(json, 'foo') FROM jsons;
SELECT simpleJSONHas(json, 'a') FROM jsons;
-- simpleJSONExtractString --
SELECT simpleJSONExtractUInt(json, 'a') FROM jsons;
SELECT simpleJSONExtractUInt(json, 'not exsits') FROM jsons;
-- simpleJSONExtractInt --
SELECT simpleJSONExtractInt(json, 'a') FROM jsons;
SELECT simpleJSONExtractInt(json, 'not exsits') FROM jsons;
-- simpleJSONExtractFloat --
SELECT simpleJSONExtractFloat(json, 'a') FROM jsons;
SELECT simpleJSONExtractFloat(json, 'not exsits') FROM jsons;
-- simpleJSONExtractBool --
SELECT simpleJSONExtractBool(json, 'a') FROM jsons;
SELECT simpleJSONExtractBool(json, 'not exsits') FROM jsons;
-- positionUTF8 --
SELECT positionUTF8(json, 'a') FROM jsons;
-- positionCaseInsensitiveUTF8 --
SELECT positionCaseInsensitiveUTF8(json, 'A') FROM jsons;
-- positionCaseInsensitive --
SELECT positionCaseInsensitive(json, 'A') FROM jsons;
