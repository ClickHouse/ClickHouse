-- Test that reverseUTF8 does not crash on invalid UTF-8 (truncated multi-byte sequences)
SELECT reverseUTF8(unhex('C0')) FORMAT Null;
SELECT reverseUTF8(unhex('E0')) FORMAT Null;
SELECT reverseUTF8(unhex('F0')) FORMAT Null;
SELECT reverseUTF8(unhex('E0A0')) FORMAT Null;
SELECT reverseUTF8(unhex('F09F')) FORMAT Null;
SELECT reverseUTF8(unhex('F09F98')) FORMAT Null;

-- The original crash query from the AST fuzzer
SELECT DISTINCT reverseUTF8(maxMergeDistinct(x) IGNORE NULLS), toNullable(1) FROM (SELECT DISTINCT dictHas(tuple(toUInt16(NULL)), 13, toUInt32(6), NULL), CAST(concat(unhex('00001000'), randomString(intDiv(1048576, toNullable(1))), toLowCardinality(toFixedString('\0', 1))), 'AggregateFunction(max, String)') AS x) WITH TOTALS FORMAT Null; -- { serverError BAD_ARGUMENTS }

-- Verify correct behavior on valid UTF-8
SELECT reverseUTF8('ClickHouse');
SELECT reverseUTF8('Привет');
SELECT reverseUTF8('こんにちは');
