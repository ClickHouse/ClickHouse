SELECT accurateCast(-1, 'UInt8'); -- { serverError 70 }
SELECT accurateCast(5, 'UInt8');
SELECT accurateCast(257, 'UInt8'); -- { serverError 70 }
SELECT accurateCast(-1, 'UInt16'); -- { serverError 70 }
SELECT accurateCast(5, 'UInt16');
SELECT accurateCast(65536, 'UInt16'); -- { serverError 70 }
SELECT accurateCast(-1, 'UInt32'); -- { serverError 70 }
SELECT accurateCast(5, 'UInt32');
SELECT accurateCast(4294967296, 'UInt32'); -- { serverError 70 }
SELECT accurateCast(-1, 'UInt64'); -- { serverError 70 }
SELECT accurateCast(5, 'UInt64');
SELECT accurateCast(-1, 'UInt256'); -- { serverError 70 }
SELECT accurateCast(5, 'UInt256');

SELECT accurateCast(-129, 'Int8'); -- { serverError 70 }
SELECT accurateCast(5, 'Int8');
SELECT accurateCast(128, 'Int8'); -- { serverError 70 }

SELECT accurateCast(10, 'Decimal32(9)'); -- { serverError 407 }
SELECT accurateCast(1, 'Decimal32(9)');
SELECT accurateCast(-10, 'Decimal32(9)'); -- { serverError 407 }

SELECT accurateCast('123', 'FixedString(2)'); -- { serverError 131 }
SELECT accurateCast('12', 'FixedString(2)');

SELECT accurateCast(-1, 'DateTime');   -- { serverError 70 }
SELECT accurateCast('1xxx', 'DateTime');   -- { serverError 41 }
SELECT accurateCast('2023-05-30 14:38:20', 'DateTime');
SELECT accurateCast(19, 'DateTime');

SELECT accurateCast(-1, 'Date');   -- { serverError 70 }
SELECT accurateCast('1xxx', 'Date');   -- { serverError 38 }
SELECT accurateCast('2023-05-30', 'Date');
SELECT accurateCast(19, 'Date');
