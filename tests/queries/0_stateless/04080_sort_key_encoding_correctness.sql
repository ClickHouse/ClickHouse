-- Baseline correctness tests for sort key encoding

SELECT '-- Signed integers';
SELECT x FROM (SELECT arrayJoin([toInt8(-128), toInt8(127), toInt8(0), toInt8(-1), toInt8(1)]) AS x) ORDER BY x ASC;
SELECT x FROM (SELECT arrayJoin([toInt8(-128), toInt8(127), toInt8(0), toInt8(-1), toInt8(1)]) AS x) ORDER BY x DESC;
SELECT x FROM (SELECT arrayJoin([toInt64(-9223372036854775808), toInt64(9223372036854775807), toInt64(0), toInt64(-1), toInt64(1)]) AS x) ORDER BY x ASC;
SELECT x FROM (SELECT arrayJoin([toInt64(-9223372036854775808), toInt64(9223372036854775807), toInt64(0), toInt64(-1), toInt64(1)]) AS x) ORDER BY x DESC;

SELECT '-- Float specials';
SELECT x FROM (SELECT arrayJoin([toFloat64(-inf), toFloat64(-1.0), toFloat64(-0.0), toFloat64(0.0), toFloat64(1.0), toFloat64(inf), toFloat64(nan)]) AS x) ORDER BY x ASC, reinterpretAsUInt64(x) SETTINGS optimize_redundant_functions_in_order_by = 0;
SELECT x FROM (SELECT arrayJoin([toFloat64(-inf), toFloat64(-1.0), toFloat64(-0.0), toFloat64(0.0), toFloat64(1.0), toFloat64(inf), toFloat64(nan)]) AS x) ORDER BY x DESC, reinterpretAsUInt64(x) SETTINGS optimize_redundant_functions_in_order_by = 0;

SELECT '-- Negative zero';
SELECT toFloat64(-0.0) = toFloat64(0.0) AS eq, toFloat64(-0.0) < toFloat64(0.0) AS lt;

SELECT '-- Nullable float with NaN';
SELECT x FROM (SELECT arrayJoin([toNullable(toFloat64(nan)), toNullable(toFloat64(-inf)), toNullable(toFloat64(1.0)), toNullable(toFloat64(-1.0)), CAST(NULL AS Nullable(Float64))]) AS x) ORDER BY x ASC NULLS FIRST;
SELECT x FROM (SELECT arrayJoin([toNullable(toFloat64(nan)), toNullable(toFloat64(-inf)), toNullable(toFloat64(1.0)), toNullable(toFloat64(-1.0)), CAST(NULL AS Nullable(Float64))]) AS x) ORDER BY x ASC NULLS LAST;
SELECT x FROM (SELECT arrayJoin([toNullable(toFloat64(nan)), toNullable(toFloat64(-inf)), toNullable(toFloat64(1.0)), toNullable(toFloat64(-1.0)), CAST(NULL AS Nullable(Float64))]) AS x) ORDER BY x DESC NULLS FIRST;
SELECT x FROM (SELECT arrayJoin([toNullable(toFloat64(nan)), toNullable(toFloat64(-inf)), toNullable(toFloat64(1.0)), toNullable(toFloat64(-1.0)), CAST(NULL AS Nullable(Float64))]) AS x) ORDER BY x DESC NULLS LAST;

SELECT '-- Strings with \\0 bytes';
SELECT hex(x) FROM (SELECT arrayJoin([unhex('00616263'), unhex(''), unhex('616263'), unhex('6162'), unhex('61620063')]) AS x) ORDER BY x ASC;
SELECT hex(x) FROM (SELECT arrayJoin([unhex('00616263'), unhex(''), unhex('616263'), unhex('6162'), unhex('61620063')]) AS x) ORDER BY x DESC;

SELECT '-- Shared prefix strings';
SELECT x FROM (SELECT arrayJoin(['abcdefgh1', 'abcdefgh2', 'abcdefgh', 'abcdefg', 'abcdefgh11']) AS x) ORDER BY x ASC;
SELECT x FROM (SELECT arrayJoin(['abcdefgh1', 'abcdefgh2', 'abcdefgh', 'abcdefg', 'abcdefgh11']) AS x) ORDER BY x DESC;

SELECT '-- Empty strings';
SELECT x FROM (SELECT arrayJoin(['', 'a', 'ab', 'b']) AS x) ORDER BY x ASC;
SELECT x FROM (SELECT arrayJoin(['', 'a', 'ab', 'b']) AS x) ORDER BY x DESC;

SELECT '-- LowCardinality(String)';
SELECT x FROM (SELECT arrayJoin([toLowCardinality(''), toLowCardinality('a'), toLowCardinality('ab'), toLowCardinality('b')]) AS x) ORDER BY x ASC;
SELECT x FROM (SELECT arrayJoin([toLowCardinality(''), toLowCardinality('a'), toLowCardinality('ab'), toLowCardinality('b')]) AS x) ORDER BY x DESC;

SET allow_suspicious_low_cardinality_types = 1;
SELECT '-- LowCardinality(Nullable(UInt64))';
SELECT x FROM (SELECT arrayJoin([toLowCardinality(toNullable(toUInt64(0))), toLowCardinality(toNullable(toUInt64(1))), toLowCardinality(toNullable(toUInt64(18446744073709551615))), CAST(NULL AS LowCardinality(Nullable(UInt64)))]) AS x) ORDER BY x ASC;
SELECT x FROM (SELECT arrayJoin([toLowCardinality(toNullable(toUInt64(0))), toLowCardinality(toNullable(toUInt64(1))), toLowCardinality(toNullable(toUInt64(18446744073709551615))), CAST(NULL AS LowCardinality(Nullable(UInt64)))]) AS x) ORDER BY x DESC;
SET allow_suspicious_low_cardinality_types = 0;

SELECT '-- Decimal negatives';
SELECT x FROM (SELECT arrayJoin([toDecimal64(-999.99, 2), toDecimal64(-0.01, 2), toDecimal64(0, 2), toDecimal64(0.01, 2), toDecimal64(999.99, 2)]) AS x) ORDER BY x ASC;
SELECT x FROM (SELECT arrayJoin([toDecimal64(-999.99, 2), toDecimal64(-0.01, 2), toDecimal64(0, 2), toDecimal64(0.01, 2), toDecimal64(999.99, 2)]) AS x) ORDER BY x DESC;

SELECT '-- UUID';
SELECT x FROM (SELECT arrayJoin([toUUID('00000000-0000-0000-0000-000000000000'), toUUID('00000000-0000-0000-0000-000000000001'), toUUID('ffffffff-ffff-ffff-ffff-ffffffffffff'), toUUID('12345678-1234-1234-1234-123456789abc')]) AS x) ORDER BY x ASC;
SELECT x FROM (SELECT arrayJoin([toUUID('00000000-0000-0000-0000-000000000000'), toUUID('00000000-0000-0000-0000-000000000001'), toUUID('ffffffff-ffff-ffff-ffff-ffffffffffff'), toUUID('12345678-1234-1234-1234-123456789abc')]) AS x) ORDER BY x DESC;

SELECT '-- Mixed ASC/DESC';
SELECT a, b FROM (SELECT number % 3 AS a, number % 5 AS b FROM numbers(15)) ORDER BY a ASC, b DESC;

SELECT '-- NULLS FIRST/LAST';
SELECT x FROM (SELECT arrayJoin([toNullable(toInt64(3)), toNullable(toInt64(-1)), CAST(NULL AS Nullable(Int64)), toNullable(toInt64(0))]) AS x) ORDER BY x ASC NULLS FIRST;
SELECT x FROM (SELECT arrayJoin([toNullable(toInt64(3)), toNullable(toInt64(-1)), CAST(NULL AS Nullable(Int64)), toNullable(toInt64(0))]) AS x) ORDER BY x ASC NULLS LAST;
SELECT x FROM (SELECT arrayJoin([toNullable(toInt64(3)), toNullable(toInt64(-1)), CAST(NULL AS Nullable(Int64)), toNullable(toInt64(0))]) AS x) ORDER BY x DESC NULLS FIRST;
SELECT x FROM (SELECT arrayJoin([toNullable(toInt64(3)), toNullable(toInt64(-1)), CAST(NULL AS Nullable(Int64)), toNullable(toInt64(0))]) AS x) ORDER BY x DESC NULLS LAST;

SELECT '-- FixedString';
SELECT hex(x) FROM (SELECT arrayJoin([toFixedString(unhex('000000'), 3), toFixedString(unhex('000001'), 3), toFixedString(unhex('010000'), 3), toFixedString(unhex('ffffff'), 3)]) AS x) ORDER BY x ASC;
SELECT hex(x) FROM (SELECT arrayJoin([toFixedString(unhex('000000'), 3), toFixedString(unhex('000001'), 3), toFixedString(unhex('010000'), 3), toFixedString(unhex('ffffff'), 3)]) AS x) ORDER BY x DESC;

SELECT '-- Tuple sort key';
SELECT * FROM (SELECT number % 3 AS a, number % 5 AS b FROM numbers(10)) ORDER BY (a, b);

SELECT '-- Array sort key';
SELECT x FROM (SELECT arrayJoin([[1, 2, 3], [1, 2], [1, 3], [0], []]) AS x) ORDER BY x ASC;
