SELECT accurateCast(-1, 'UInt8'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(5, 'UInt8');
SELECT accurateCast(257, 'UInt8'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(-1, 'UInt16'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(5, 'UInt16');
SELECT accurateCast(65536, 'UInt16'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(-1, 'UInt32'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(5, 'UInt32');
SELECT accurateCast(4294967296, 'UInt32'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(-1, 'UInt64'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(5, 'UInt64');
SELECT accurateCast(-1, 'UInt256'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(5, 'UInt256');

SELECT accurateCast(-129, 'Int8'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(5, 'Int8');
SELECT accurateCast(128, 'Int8'); -- { serverError CANNOT_CONVERT_TYPE }

SELECT accurateCast('-1', 'UInt8'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast('5', 'UInt8');
SELECT accurateCast('257', 'UInt8'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast('-1', 'UInt16'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast('5', 'UInt16');
SELECT accurateCast('65536', 'UInt16'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast('-1', 'UInt32'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast('5', 'UInt32');
SELECT accurateCast('4294967296', 'UInt32'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast('-1', 'UInt64'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast('5', 'UInt64');
SELECT accurateCast('-129', 'Int8'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast('5', 'Int8');
SELECT accurateCast('128', 'Int8'); -- { serverError CANNOT_PARSE_TEXT }

SELECT accurateCast(10, 'Decimal32(9)'); -- { serverError DECIMAL_OVERFLOW }
SELECT accurateCast(1, 'Decimal32(9)');
SELECT accurateCast(-10, 'Decimal32(9)'); -- { serverError DECIMAL_OVERFLOW }

SELECT accurateCast('123', 'FixedString(2)'); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT accurateCast('12', 'FixedString(2)');

SELECT accurateCast(-1, 'DateTime');   -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(0xFFFFFFFF + 1, 'DateTime');   -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast('1xxx', 'DateTime');   -- { serverError CANNOT_PARSE_DATETIME }
SELECT accurateCast('2023-05-30 14:38:20', 'DateTime');
SELECT toString(accurateCast(19, 'DateTime'), 'UTC');

SELECT accurateCast(-1, 'Date');   -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(0xFFFFFFFF + 1, 'Date');   -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast('1xxx', 'Date');   -- { serverError CANNOT_PARSE_DATE }
SELECT accurateCast('2023-05-30', 'Date');
SELECT accurateCast(19, 'Date');

select accurateCast('test', 'Nullable(Bool)');   -- { serverError CANNOT_PARSE_BOOL }
select accurateCast('test', 'Bool');   -- { serverError CANNOT_PARSE_BOOL }
select accurateCast('truex', 'Bool');   -- { serverError CANNOT_PARSE_BOOL }
select accurateCast('xfalse', 'Bool');   -- { serverError CANNOT_PARSE_BOOL }
select accurateCast('true', 'Bool');
select accurateCast('false', 'Bool');
select accurateCast('1', 'Bool');
select accurateCast('0', 'Bool');
select accurateCast(1, 'Bool');
select accurateCast(0, 'Bool');

select accurateCast('test', 'Nullable(IPv4)');
select accurateCast('test', 'IPv4');   -- { serverError CANNOT_PARSE_IPV4 }
select accurateCast('2001:db8::1', 'IPv4');   -- { serverError CANNOT_PARSE_IPV4 }
select accurateCast('::ffff:192.0.2.1', 'IPv4');   -- { serverError CANNOT_PARSE_IPV4 }
select accurateCast('192.0.2.1', 'IPv4');
select accurateCast('192.0.2.1x', 'IPv4');   -- { serverError CANNOT_PARSE_IPV4 }

select accurateCast('test', 'Nullable(IPv6)');
select accurateCast('test', 'IPv6');   -- { serverError CANNOT_PARSE_IPV6 }
select accurateCast('192.0.2.1', 'IPv6');
select accurateCast('2001:db8::1', 'IPv6');
select accurateCast('2001:db8::1x', 'IPv6');   -- { serverError CANNOT_PARSE_IPV6 }
