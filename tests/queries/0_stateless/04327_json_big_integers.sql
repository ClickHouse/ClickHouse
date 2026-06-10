-- Tags: no-fasttest
-- Tests for big integer parsing (integers exceeding 64-bit range) in JSON.
-- simdjson now writes such numbers as strings on the tape so that
-- ClickHouse can parse them into wider integer types (UInt128, Int128, etc.).

SET allow_experimental_json_type = 1;

SELECT '-- Positive big integers into UInt128';
SELECT '{"a" : 123456789123456789123456789}'::JSON(a UInt128) AS j, j.a;
SELECT '{"a" : 340282366920938463463374607431768211455}'::JSON(a UInt128) AS j, j.a; -- max UInt128

SELECT '-- Negative big integers into Int128';
SELECT '{"a" : -123456789123456789123456789}'::JSON(a Int128) AS j, j.a;

SELECT '-- Very large integers into UInt256';
SELECT '{"a" : 123456789123456789123456789123456789123456789}'::JSON(a UInt256) AS j, j.a;

SELECT '-- Very large negative integers into Int256';
SELECT '{"a" : -123456789123456789123456789123456789123456789}'::JSON(a Int256) AS j, j.a;

SELECT '-- Normal integers still work correctly';
SELECT '{"a" : 42}'::JSON(a UInt64) AS j, j.a;
SELECT '{"a" : -42}'::JSON(a Int64) AS j, j.a;
SELECT '{"a" : 18446744073709551615}'::JSON(a UInt64) AS j, j.a; -- max UInt64
SELECT '{"a" : 9223372036854775807}'::JSON(a Int64) AS j, j.a; -- max Int64

SELECT '-- Big integers coerced to smaller types that can hold them';
SELECT '{"a" : 18446744073709551616}'::JSON(a UInt128) AS j, j.a; -- UInt64 max + 1

SELECT '-- Mixed document: normal and big integers together';
SELECT '{"a" : 42, "b" : 123456789123456789123456789}'::JSON(a UInt64, b UInt128) AS j, j.a, j.b;

SELECT '-- Big integers in arrays';
SELECT '{"a" : [123456789123456789123456789, 987654321987654321987654321]}'::JSON(a Array(UInt128)) AS j, j.a;

SELECT '-- Big integer with JSONExtract';
SELECT JSONExtract('{"a" : 123456789123456789123456789}', 'a', 'UInt128');
SELECT JSONExtract('{"a" : -123456789123456789123456789}', 'a', 'Int128');

SELECT '-- Big integer into String (should work, stored as string)';
SELECT '{"a" : 123456789123456789123456789}'::JSON(a String) AS j, j.a;

SELECT '-- Untyped path: big integer inferred as String in dynamic JSON';
SELECT '{"a" : 123456789123456789123456789}'::JSON AS j, j.a, dynamicType(j.a);

SELECT '-- Big integer does not break extraction of other fields (issue #81132)';
SELECT JSONExtractString('{"id":"123","big_number":100000000000000000000000000}', 'id');

SELECT '-- isValidJSON with big integer (issue #45782)';
SELECT isValidJSON('{"value": 7230000000000000000000000000000000000000000000000000000000000000000000000000}');

SELECT '-- JSONExtract functions with big integers (issue #49895)';
SELECT
    JSONExtractString('{ "a": -57783588950605040000 }', 'a'),
    JSONExtractFloat('{ "a": -57783588950605040000 }', 'a');
