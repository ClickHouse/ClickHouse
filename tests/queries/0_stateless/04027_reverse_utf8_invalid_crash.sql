-- Test that reverseUTF8 does not crash on invalid UTF-8 input.
-- A multi-byte UTF-8 lead byte at the end of a string without enough following bytes
-- must not cause an out-of-bounds access.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=8f8e0d3934ac11a8887020d917ca88aa0727f3ea&name_0=MasterCI&name_1=AST%20fuzzer%20%28amd_debug%29

-- String ending with 0xF0 (4-byte UTF-8 lead byte) but only 1 byte present
SELECT length(reverseUTF8(unhex('C0'))) FORMAT Null;
SELECT length(reverseUTF8(unhex('E0'))) FORMAT Null;
SELECT length(reverseUTF8(unhex('F0'))) FORMAT Null;

-- Multi-byte lead byte at end of an otherwise valid string
SELECT length(reverseUTF8(concat('hello', unhex('F0')))) FORMAT Null;
SELECT length(reverseUTF8(concat('hello', unhex('E0')))) FORMAT Null;
SELECT length(reverseUTF8(concat('hello', unhex('C0')))) FORMAT Null;

-- Incomplete multi-byte sequences
SELECT length(reverseUTF8(unhex('F0A0'))) FORMAT Null;
SELECT length(reverseUTF8(unhex('F0A0B0'))) FORMAT Null;
SELECT length(reverseUTF8(unhex('E0A0'))) FORMAT Null;

-- The original crashing query (simplified)
SELECT reverseUTF8(randomString(100)) FORMAT Null;
