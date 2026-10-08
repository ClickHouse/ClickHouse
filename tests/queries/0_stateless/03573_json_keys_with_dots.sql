SET json_type_escape_dots_in_keys=1;
SET enable_analyzer=1;

SELECT '{"a" : {"b" : 42}}'::JSON AS json1, '{"a.b" : 42}'::JSON AS json2, JSONAllPaths(json1), JSONAllPaths(json2);
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, JSONAllPaths(json);
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, json.`a%2Eb`, json.a.b;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, json.`a%2Eb`, json.`a.b`, json.a.b;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON(`a%2Eb` UInt8) as json, json.`a%2Eb`, toTypeName(json.`a%2Eb`);

