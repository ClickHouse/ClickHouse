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

SELECT '-- Multi-path types with LowCardinality(String) elements on Dynamic input (issue: LOGICAL_ERROR, STID 4811-66fc)';
SELECT toTypeName(JSON_QUERY('{"a": 1, "b": 2}'::Dynamic, tuple('$.a', toLowCardinality('$.b'))));
SELECT toTypeName(JSON_VALUE('{"a": 1, "b": 2}'::Dynamic, tuple('$.a', toLowCardinality('$.b'))));
SELECT toTypeName(JSON_EXISTS('{"a": 1, "b": 2}'::Dynamic, tuple('$.a', toLowCardinality('$.b'))));

SELECT toTypeName(JSON_EXISTS('{"a": 1}'::Dynamic, tuple(toLowCardinality('$.a'), toLowCardinality('$.b'))));
SELECT toTypeName(JSON_EXISTS('{"a": 1}'::Dynamic, arrayMap(x -> toLowCardinality(x), ['$.a', '$.b'])));
SELECT toTypeName(JSON_EXISTS('{"a": 1}'::Dynamic, toLowCardinality('$.a')));

SELECT JSON_QUERY('{"a": 1, "b": 2}'::Dynamic, tuple('$.a', toLowCardinality('$.b')));
SELECT JSON_VALUE('{"a": 1, "b": 2}'::Dynamic, tuple('$.a', toLowCardinality('$.b')));
SELECT JSON_EXISTS('{"a": 1, "b": 2}'::Dynamic, tuple('$.a', toLowCardinality('$.b')));

-- Single-element LowCardinality tuple path (issue #110345)
SELECT JSON_EXISTS(CAST('{"a":1}', 'Dynamic'), tuple(toLowCardinality('$.a')));

SELECT '-- Multi-path arg arriving via a scalar subquery: a scalar Tuple subquery is Nullable(Tuple), a scalar Array subquery is Array (issue: LOGICAL_ERROR, STID 4811-66fc)';
SELECT toTypeName(JSON_EXISTS('{"a": 1, "b": 2}'::Dynamic, (SELECT tuple('$.a', '$.b'))));
SELECT toTypeName(JSON_VALUE('{"a": 1, "b": 2}'::Dynamic, (SELECT tuple('$.a', '$.b'))));
SELECT toTypeName(JSON_QUERY('{"a": 1, "b": 2}'::Dynamic, (SELECT tuple('$.a', '$.b'))));
SELECT toTypeName(JSON_EXISTS('{"a": 1}'::Dynamic, (SELECT tuple('$.b'))));
SELECT toTypeName(JSON_EXISTS('{"a": 1}'::Dynamic, (SELECT array('$.a', '$.b'))));

SELECT JSON_EXISTS('{"a": 1, "b": 2}'::Dynamic, (SELECT tuple('$.a', '$.b$.b$.b$.b')));
SELECT DISTINCT JSON_EXISTS('2299-12-31'::Dynamic, (SELECT tuple('$.b')));
SELECT JSON_VALUE('{"a": 1, "b": 2}'::Dynamic, (SELECT tuple('$.a', '$.b')));
SELECT JSON_QUERY('{"a": 1, "b": 2}'::Dynamic, (SELECT tuple('$.a', '$.b')));

SELECT '-- Nested wrapped path node (Tuple with a Nullable(Tuple) element): rejected as a non-path type, identically on the normal and Dynamic paths';
SELECT JSON_EXISTS('{"a": 1, "b": 2, "c": 3}', tuple((SELECT tuple('$.a', '$.b')), '$.c')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSON_EXISTS('{"a": 1, "b": 2, "c": 3}'::Dynamic, tuple((SELECT tuple('$.a', '$.b')), '$.c')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSON_VALUE('{"a": 1, "b": 2, "c": 3}'::Dynamic, tuple((SELECT tuple('$.a', '$.b')), '$.c')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Nested plain-Array path node stays a valid multi-path structure (a scalar Array subquery is not Nullable)';
SELECT JSON_EXISTS('{"a": 1}'::Dynamic, [(SELECT ['$.a'])]);
SELECT toTypeName(JSON_EXISTS('{"a": 1}'::Dynamic, [(SELECT ['$.a'])]));
