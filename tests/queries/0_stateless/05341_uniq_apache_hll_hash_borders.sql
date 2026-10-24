-- Tags: no-fasttest, no-cpu-s390x
-- ^ DataSketches is not built in fast-test builds.

-- Pins how every supported argument type is hashed, on the border values of its range.
-- Each expected state was produced by the Apache DataSketches C++ library: integers are
-- widened by value to 64 bits (as Java `update(long)` and Python `update(int)` do),
-- floats are widened to `double` with `-0.0` and `NaN` canonicalized, and strings and
-- fixed-size types are hashed as raw bytes. A state fed with up to eight values stays
-- a coupon list, so the bytes depend only on the values and their order.

SELECT 'integers';
SELECT 'Int8', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(CAST(['-128', '-1', '0', '1', '127'], 'Array(Int8)')) AS x) SETTINGS max_threads = 1;
SELECT 'UInt8', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(CAST(['0', '127', '128', '255'], 'Array(UInt8)')) AS x) SETTINGS max_threads = 1;
SELECT 'Int16', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(CAST(['-32768', '-1', '0', '32767'], 'Array(Int16)')) AS x) SETTINGS max_threads = 1;
SELECT 'UInt16', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(CAST(['0', '32767', '32768', '65535'], 'Array(UInt16)')) AS x) SETTINGS max_threads = 1;
SELECT 'Int32', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(CAST(['-2147483648', '-1', '0', '2147483647'], 'Array(Int32)')) AS x) SETTINGS max_threads = 1;
SELECT 'UInt32', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(CAST(['0', '2147483647', '2147483648', '4294967295'], 'Array(UInt32)')) AS x) SETTINGS max_threads = 1;
SELECT 'Int64', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(CAST(['-9223372036854775808', '-1', '0', '9223372036854775807'], 'Array(Int64)')) AS x) SETTINGS max_threads = 1;
SELECT 'UInt64', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(CAST(['0', '9223372036854775807', '9223372036854775808', '18446744073709551615'], 'Array(UInt64)')) AS x) SETTINGS max_threads = 1;

SELECT 'floats';
SELECT 'Float64', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin([reinterpretAsFloat64(unhex('0000000000000000')), reinterpretAsFloat64(unhex('0000000000000080')), reinterpretAsFloat64(unhex('000000000000F83F')), reinterpretAsFloat64(unhex('000000000000F87F')), reinterpretAsFloat64(unhex('000000000000F07F')), reinterpretAsFloat64(unhex('000000000000F0FF')), reinterpretAsFloat64(unhex('0100000000000000')), reinterpretAsFloat64(unhex('FFFFFFFFFFFFEF7F'))]) AS x) SETTINGS max_threads = 1;
SELECT 'Float32', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin([reinterpretAsFloat32(unhex('00000000')), reinterpretAsFloat32(unhex('00000080')), reinterpretAsFloat32(unhex('CDCCCC3D')), reinterpretAsFloat32(unhex('0000C03F')), reinterpretAsFloat32(unhex('0000C07F')), reinterpretAsFloat32(unhex('0000807F')), reinterpretAsFloat32(unhex('000080FF')), reinterpretAsFloat32(unhex('FFFF7F7F')), reinterpretAsFloat32(unhex('01000000'))]) AS x) SETTINGS max_threads = 1;
SELECT 'BFloat16', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin([toBFloat16(1.5), toBFloat16(-2.0), toBFloat16(0.5), toBFloat16(256.0)]) AS x) SETTINGS max_threads = 1;

SELECT 'strings';
SELECT 'String', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(['a', 'abc', 'привет', 'a\0b', ' ', '\xFF']) AS x) SETTINGS max_threads = 1;
SELECT 'FixedString(5)', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(CAST(['a', 'abc', 'abcde'], 'Array(FixedString(5))')) AS x) SETTINGS max_threads = 1;

SELECT 'dates and times';
SELECT 'Date', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(arrayMap(v -> toDate(v), [0, 32767, 32768, 65535])) AS x) SETTINGS max_threads = 1;
SELECT 'Date32', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(arrayMap(v -> toDate32(v), [-25567, -1, 0, 1, 120529])) AS x) SETTINGS max_threads = 1;
SELECT 'DateTime', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(arrayMap(v -> toDateTime(v, 'UTC'), [0, 2147483647, 2147483648, 4294967295])) AS x) SETTINGS max_threads = 1;
SELECT 'DateTime64(0)', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(arrayMap(v -> toDateTime64(v, 0, 'UTC'), CAST(['-2208988799', '-1', '0', '1', '1577836800'], 'Array(Int64)'))) AS x) SETTINGS max_threads = 1;
SELECT 'DateTime64(3)', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(arrayMap(v -> fromUnixTimestamp64Milli(v, 'UTC'), CAST(['-2208988799999', '-1', '0', '1', '1577836800123'], 'Array(Int64)'))) AS x) SETTINGS max_threads = 1;
SELECT 'DateTime64(6)', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(arrayMap(v -> fromUnixTimestamp64Micro(v, 'UTC'), CAST(['-2208988799999999', '-1', '0', '1', '1577836800123456'], 'Array(Int64)'))) AS x) SETTINGS max_threads = 1;

SELECT 'network and identifiers';
SELECT 'IPv4', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(CAST(['0.0.0.0', '127.255.255.255', '128.0.0.0', '192.168.1.1', '255.255.255.255'], 'Array(IPv4)')) AS x) SETTINGS max_threads = 1;
SELECT 'IPv6', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(CAST(['::', '::1', '2001:db8::1', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'], 'Array(IPv6)')) AS x) SETTINGS max_threads = 1;
SELECT 'UUID', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(CAST(['00000000-0000-0000-0000-000000000000', '01234567-89ab-cdef-0123-456789abcdef', 'ffffffff-ffff-ffff-ffff-ffffffffffff'], 'Array(UUID)')) AS x) SETTINGS max_threads = 1;
SELECT 'Enum8', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(CAST(['min', 'zero', 'max'], 'Array(Enum8(\'min\' = -128, \'zero\' = 0, \'max\' = 127))')) AS x) SETTINGS max_threads = 1;
SELECT 'Enum16', hex(toString(uniqApacheHLLState(x))) FROM (SELECT arrayJoin(CAST(['min', 'zero', 'max'], 'Array(Enum16(\'min\' = -32768, \'zero\' = 0, \'max\' = 32767))')) AS x) SETTINGS max_threads = 1;

SELECT 'sketch modes and parameters';
SELECT 'empty, lg_k 12, HLL_4', hex(toString(uniqApacheHLLState(number))) FROM numbers(0) SETTINGS max_threads = 1;
SELECT 'empty, lg_k 4, HLL_4', hex(toString(uniqApacheHLLState(4, 'HLL_4')(number))) FROM numbers(0) SETTINGS max_threads = 1;
SELECT 'empty, lg_k 21, HLL_8', hex(toString(uniqApacheHLLState(21, 'HLL_8')(number))) FROM numbers(0) SETTINGS max_threads = 1;
SELECT 'empty, lg_k 12, HLL_6', hex(toString(uniqApacheHLLState(12, 'HLL_6')(number))) FROM numbers(0) SETTINGS max_threads = 1;
SELECT 'list, 8 values', hex(toString(uniqApacheHLLState(number))) FROM numbers(8) SETTINGS max_threads = 1;
SELECT 'set, 9 values', hex(toString(uniqApacheHLLState(number))) FROM numbers(9) SETTINGS max_threads = 1;
SELECT 'hll, lg_k 4, HLL_4, 100 values', hex(toString(uniqApacheHLLState(4, 'HLL_4')(number))) FROM numbers(100) SETTINGS max_threads = 1;
SELECT 'hll, lg_k 4, HLL_6, 100 values', hex(toString(uniqApacheHLLState(4, 'HLL_6')(number))) FROM numbers(100) SETTINGS max_threads = 1;
SELECT 'hll, lg_k 4, HLL_8, 100 values', hex(toString(uniqApacheHLLState(4, 'HLL_8')(number))) FROM numbers(100) SETTINGS max_threads = 1;

SELECT 'merging empty states gives an empty sketch';
SELECT hex(toString(uniqApacheHLLMergeState(s))) FROM (SELECT uniqApacheHLLState(number) AS s FROM numbers(0));

SELECT 'integers are widened by value';
SELECT 'Int8 -1 = Int64 -1', hex(toString(uniqApacheHLLState(toInt8(-1)))) = hex(toString(uniqApacheHLLState(toInt64(-1))));
SELECT 'Int32 min = Int64 min', hex(toString(uniqApacheHLLState(toInt32(-2147483648)))) = hex(toString(uniqApacheHLLState(toInt64(-2147483648))));
SELECT 'UInt8 200 = UInt64 200', hex(toString(uniqApacheHLLState(toUInt8(200)))) = hex(toString(uniqApacheHLLState(toUInt64(200))));
SELECT 'UInt8 200 = Int16 200', hex(toString(uniqApacheHLLState(toUInt8(200)))) = hex(toString(uniqApacheHLLState(toInt16(200))));
SELECT 'UInt32 max != Int32 -1', hex(toString(uniqApacheHLLState(toUInt32(4294967295)))) != hex(toString(uniqApacheHLLState(toInt32(-1))));
SELECT 'UInt64 max = Int64 -1', hex(toString(uniqApacheHLLState(toUInt64(18446744073709551615)))) = hex(toString(uniqApacheHLLState(toInt64(-1))));

SELECT 'floating point values are canonicalized';
SELECT '-0 = 0', hex(toString(uniqApacheHLLState(toFloat64('-0')))) = hex(toString(uniqApacheHLLState(toFloat64('0'))));
SELECT 'Float32 1.5 = Float64 1.5', hex(toString(uniqApacheHLLState(toFloat32(1.5)))) = hex(toString(uniqApacheHLLState(toFloat64(1.5))));
SELECT 'Float32 0.1 widened', hex(toString(uniqApacheHLLState(toFloat32(0.1)))) = hex(toString(uniqApacheHLLState(toFloat64(toFloat32(0.1)))));
SELECT 'Float32 0.1 != Float64 0.1', hex(toString(uniqApacheHLLState(toFloat32(0.1)))) != hex(toString(uniqApacheHLLState(toFloat64(0.1))));
SELECT 'inf != -inf', hex(toString(uniqApacheHLLState(toFloat64('inf')))) != hex(toString(uniqApacheHLLState(toFloat64('-inf'))));
SELECT 'NaN payloads', uniqApacheHLL(x) FROM (SELECT arrayJoin([reinterpretAsFloat64(unhex('000000000000F87F')), reinterpretAsFloat64(unhex('010000000000F87F')), reinterpretAsFloat64(unhex('000000000000F8FF'))]) AS x);

SELECT 'types share the hash of their underlying value';
SELECT 'Enum8 max = Int8 127', hex(toString(uniqApacheHLLState(CAST('max', 'Enum8(\'max\' = 127)')))) = hex(toString(uniqApacheHLLState(toInt8(127))));
SELECT 'Date32 -1 = Int32 -1', hex(toString(uniqApacheHLLState(toDate32(toInt32(-1))))) = hex(toString(uniqApacheHLLState(toInt32(-1))));
SELECT 'Date 65535 = UInt16 65535', hex(toString(uniqApacheHLLState(toDate(65535)))) = hex(toString(uniqApacheHLLState(toUInt16(65535))));
SELECT 'DateTime 2^31 = UInt32 2^31', hex(toString(uniqApacheHLLState(toDateTime(toUInt32(2147483648), 'UTC')))) = hex(toString(uniqApacheHLLState(toUInt32(2147483648))));
SELECT 'IPv4 max = UInt32 max', hex(toString(uniqApacheHLLState(toIPv4('255.255.255.255')))) = hex(toString(uniqApacheHLLState(toUInt32(4294967295))));
SELECT 'DateTime64(3) -1 = Int64 -1', hex(toString(uniqApacheHLLState(fromUnixTimestamp64Milli(toInt64(-1), 'UTC')))) = hex(toString(uniqApacheHLLState(toInt64(-1))));
SELECT 'String abc = FixedString(3) abc', hex(toString(uniqApacheHLLState('abc'))) = hex(toString(uniqApacheHLLState(toFixedString('abc', 3))));
SELECT 'String abc != FixedString(5) abc', hex(toString(uniqApacheHLLState('abc'))) != hex(toString(uniqApacheHLLState(toFixedString('abc', 5))));
SELECT 'LowCardinality(String) = String', hex(toString(uniqApacheHLLState(toLowCardinality('abc')))) = hex(toString(uniqApacheHLLState('abc')));
