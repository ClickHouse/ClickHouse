-- Verify that JSON_QUERY, JSON_VALUE, and JSON_EXISTS return correct types (not Dynamic) when input is Dynamic.
-- https://github.com/ClickHouse/ClickHouse/issues/106461

SELECT '-- Single-path types';
SELECT toTypeName(JSON_QUERY('{"a": 1}', '$.a'));
SELECT toTypeName(JSON_VALUE('{"a": 1}', '$.a'));
SELECT toTypeName(JSON_EXISTS('{"a": 1}', '$.a'));

SELECT toTypeName(JSON_QUERY('{"a": 1}'::Dynamic, '$.a'));
SELECT toTypeName(JSON_VALUE('{"a": 1}'::Dynamic, '$.a'));
SELECT toTypeName(JSON_EXISTS('{"a": 1}'::Dynamic, '$.a'));

SELECT JSON_QUERY('{"a": 1}'::Dynamic, '$.a');
SELECT JSON_VALUE('{"a": 1}'::Dynamic, '$.a');
SELECT JSON_EXISTS('{"a": 1}'::Dynamic, '$.a');

SELECT '-- Multi-path types with Dynamic input';
SELECT toTypeName(JSON_QUERY('{"a": 1, "b": 2}'::Dynamic, tuple('$.a', '$.b')));
SELECT toTypeName(JSON_VALUE('{"a": 1, "b": 2}'::Dynamic, tuple('$.a', '$.b')));
SELECT toTypeName(JSON_EXISTS('{"a": 1, "b": 2}'::Dynamic, tuple('$.a', '$.b')));

SELECT toTypeName(JSON_QUERY('{"a": 1}'::Dynamic, array('$.a', '$.b')));
SELECT toTypeName(JSON_VALUE('{"a": 1}'::Dynamic, array('$.a', '$.b')));
SELECT toTypeName(JSON_EXISTS('{"a": 1}'::Dynamic, array('$.a', '$.b')));

SELECT JSON_QUERY('{"a": 1, "b": 2}'::Dynamic, tuple('$.a', '$.b'));
SELECT JSON_VALUE('{"a": 1, "b": 2}'::Dynamic, tuple('$.a', '$.b'));
SELECT JSON_EXISTS('{"a": 1, "b": 2}'::Dynamic, tuple('$.a', '$.b'));
