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
