SELECT accurateCastOrDefault(-1, 'UInt8'), accurateCastOrDefault(5, 'UInt8');
SELECT accurateCastOrDefault(5, 'UInt8');
SELECT accurateCastOrDefault(257, 'UInt8'),  accurateCastOrDefault(257, 'UInt8', 5);
SELECT accurateCastOrDefault(-1, 'UInt16'),  accurateCastOrDefault(-1, 'UInt16', toUInt16(5));
SELECT accurateCastOrDefault(5, 'UInt16');
SELECT accurateCastOrDefault(65536, 'UInt16'),  accurateCastOrDefault(65536, 'UInt16', toUInt16(5));
SELECT accurateCastOrDefault(-1, 'UInt32'),  accurateCastOrDefault(-1, 'UInt32', toUInt32(5));
SELECT accurateCastOrDefault(5, 'UInt32');
SELECT accurateCastOrDefault(4294967296, 'UInt32'),  accurateCastOrDefault(4294967296, 'UInt32', toUInt32(5));
SELECT accurateCastOrDefault(-1, 'UInt64'), accurateCastOrDefault(-1, 'UInt64', toUInt64(5));
SELECT accurateCastOrDefault(5, 'UInt64');
SELECT accurateCastOrDefault(-1, 'UInt256'), accurateCastOrDefault(-1, 'UInt256', toUInt256(5));
SELECT accurateCastOrDefault(5, 'UInt256');
SELECT accurateCastOrDefault(-129, 'Int8'), accurateCastOrDefault(-129, 'Int8', toInt8(5));
SELECT accurateCastOrDefault(5, 'Int8');
SELECT accurateCastOrDefault(128, 'Int8'),  accurateCastOrDefault(128, 'Int8', toInt8(5));

SELECT accurateCastOrDefault(10, 'Decimal32(9)'), accurateCastOrDefault(10, 'Decimal32(9)', toDecimal32(2, 9));
SELECT accurateCastOrDefault(1, 'Decimal32(9)');
SELECT accurateCastOrDefault(-10, 'Decimal32(9)'), accurateCastOrDefault(-10, 'Decimal32(9)', toDecimal32(2, 9));

SELECT accurateCastOrDefault('123', 'FixedString(2)'), accurateCastOrDefault('123', 'FixedString(2)', cast('12', 'FixedString(2)'));

SELECT accurateCastOrDefault(inf, 'Int64'), accurateCastOrDefault(inf, 'Int64', toInt64(5));
SELECT accurateCastOrDefault(inf, 'Int128'), accurateCastOrDefault(inf, 'Int128', toInt128(5));
SELECT accurateCastOrDefault(inf, 'Int256'), accurateCastOrDefault(inf, 'Int256', toInt256(5));
SELECT accurateCastOrDefault(nan, 'Int64'), accurateCastOrDefault(nan, 'Int64', toInt64(5));
SELECT accurateCastOrDefault(nan, 'Int128'), accurateCastOrDefault(nan, 'Int128', toInt128(5));
SELECT accurateCastOrDefault(nan, 'Int256'), accurateCastOrDefault(nan, 'Int256', toInt256(5));

SELECT accurateCastOrDefault(inf, 'UInt64'), accurateCastOrDefault(inf, 'UInt64', toUInt64(5));
SELECT accurateCastOrDefault(inf, 'UInt256'), accurateCastOrDefault(inf, 'UInt256', toUInt256(5));
SELECT accurateCastOrDefault(nan, 'UInt64'), accurateCastOrDefault(nan, 'UInt64', toUInt64(5));
SELECT accurateCastOrDefault(nan, 'UInt256'), accurateCastOrDefault(nan, 'UInt256', toUInt256(5));

SELECT accurateCastOrDefault(number + 127, 'Int8') AS x, accurateCastOrDefault(number + 127, 'Int8', toInt8(5)) AS x_with_default FROM numbers (2) ORDER BY number;

select accurateCastOrDefault('test', 'Nullable(Bool)');
select accurateCastOrDefault('test', 'Bool');
select accurateCastOrDefault('truex', 'Bool');
select accurateCastOrDefault('xfalse', 'Bool');
select accurateCastOrDefault('true', 'Bool');
select accurateCastOrDefault('false', 'Bool');
select accurateCastOrDefault('1', 'Bool');
select accurateCastOrDefault('0', 'Bool');
select accurateCastOrDefault(1, 'Bool');
select accurateCastOrDefault(0, 'Bool');

select accurateCastOrDefault('test', 'Nullable(IPv4)');
select accurateCastOrDefault('test', 'IPv4');
select accurateCastOrDefault('2001:db8::1', 'IPv4');
select accurateCastOrDefault('::ffff:192.0.2.1', 'IPv4');
select accurateCastOrDefault('192.0.2.1', 'IPv4');
select accurateCastOrDefault('192.0.2.1x', 'IPv4');

select accurateCastOrDefault('test', 'Nullable(IPv6)');
select accurateCastOrDefault('test', 'IPv6');
select accurateCastOrDefault('192.0.2.1', 'IPv6');
select accurateCastOrDefault('2001:db8::1', 'IPv6');
select accurateCastOrDefault('2001:db8::1x', 'IPv6');
