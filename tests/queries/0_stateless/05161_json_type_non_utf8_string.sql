-- Tags: no-fasttest
-- no-fasttest: asserts both JSON parsers, and the Fast test build has ENABLE_LIBRARIES=0, so no rapidjson

-- unhex('e0a4') is a truncated UTF-8 sequence; unhex('e0a4b9') completes it (U+0939) and is the
-- control row, so both payloads carry non-ASCII bytes and encoding validity is the only thing that
-- differs in kind. Every value that can hold non-ASCII bytes is hex()-ed to keep the reference ASCII.

DROP TABLE IF EXISTS docs;
DROP TABLE IF EXISTS tuples;
DROP TABLE IF EXISTS json_column;

CREATE TABLE docs (name String, obj String, arr String) ENGINE = MergeTree ORDER BY name;
INSERT INTO docs VALUES
    ('1_invalid_utf8', concat('{"a":"', unhex('e0a4'),   '"}'), concat('["', unhex('e0a4'),   '"]')),
    ('2_valid_utf8',   concat('{"a":"', unhex('e0a4b9'), '"}'), concat('["', unhex('e0a4b9'), '"]'));

CREATE TABLE tuples (name String, event Tuple(test String)) ENGINE = MergeTree ORDER BY name;
INSERT INTO tuples VALUES ('1_invalid_utf8', tuple(unhex('e0a4'))), ('2_valid_utf8', tuple(unhex('e0a4b9')));

CREATE TABLE json_column (name String, doc JSON) ENGINE = MergeTree ORDER BY name;

SELECT name, isValidUTF8(obj) FROM docs ORDER BY name;

SET allow_simdjson = 1;
SELECT 'allow_simdjson = 1';

SELECT name, hex(CAST(event AS JSON)::String) FROM tuples ORDER BY name;
SELECT name, hex(CAST(obj AS JSON)::String) FROM docs ORDER BY name;
TRUNCATE TABLE json_column;
INSERT INTO json_column SELECT name, obj FROM docs;
SELECT name, hex(doc::String) FROM json_column ORDER BY name;
SELECT name, hex(JSONExtractString(obj, 'a')), JSONHas(obj, 'a'), JSONLength(obj), isValidJSON(obj) FROM docs ORDER BY name;
SELECT name, JSONArrayLength(arr) FROM docs ORDER BY name;
SELECT name, hex(JSON_VALUE(obj, '$.a')), JSON_EXISTS(obj, '$.a') FROM docs ORDER BY name;
-- structurally broken documents are still rejected, so this cannot be satisfied by accepting everything
SELECT isValidJSON('{"a"invalid}'), JSONLength('"HX-=');

SET allow_simdjson = 0;
SELECT 'allow_simdjson = 0';

SELECT name, hex(CAST(event AS JSON)::String) FROM tuples ORDER BY name;
SELECT name, hex(CAST(obj AS JSON)::String) FROM docs ORDER BY name;
TRUNCATE TABLE json_column;
INSERT INTO json_column SELECT name, obj FROM docs;
SELECT name, hex(doc::String) FROM json_column ORDER BY name;
SELECT name, hex(JSONExtractString(obj, 'a')), JSONHas(obj, 'a'), JSONLength(obj), isValidJSON(obj) FROM docs ORDER BY name;
SELECT name, JSONArrayLength(arr) FROM docs ORDER BY name;
-- JSON_VALUE / JSON_EXISTS / JSON_QUERY have no rapidjson branch, so they reject every input here;
-- that is pre-existing and unrelated to UTF-8
SELECT name, hex(JSON_VALUE(obj, '$.a')), JSON_EXISTS(obj, '$.a') FROM docs ORDER BY name; -- { serverError NOT_IMPLEMENTED }
SELECT isValidJSON('{"a"invalid}'), JSONLength('"HX-=');

DROP TABLE docs;
DROP TABLE tuples;
DROP TABLE json_column;
