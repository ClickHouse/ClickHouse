SELECT accurateCastOrNull(-1, 'UInt8');
SELECT accurateCastOrNull(5, 'UInt8');
SELECT accurateCastOrNull(257, 'UInt8');
SELECT accurateCastOrNull(-1, 'UInt16');
SELECT accurateCastOrNull(5, 'UInt16');
SELECT accurateCastOrNull(65536, 'UInt16');
SELECT accurateCastOrNull(-1, 'UInt32');
SELECT accurateCastOrNull(5, 'UInt32');
SELECT accurateCastOrNull(4294967296, 'UInt32');
SELECT accurateCastOrNull(-1, 'UInt64');
SELECT accurateCastOrNull(5, 'UInt64');
SELECT accurateCastOrNull(-1, 'UInt256');
SELECT accurateCastOrNull(5, 'UInt256');

SELECT accurateCastOrNull(-129, 'Int8');
SELECT accurateCastOrNull(5, 'Int8');
SELECT accurateCastOrNull(128, 'Int8');

SELECT accurateCastOrNull(10, 'Decimal32(9)');
SELECT accurateCastOrNull(1, 'Decimal32(9)');
SELECT accurateCastOrNull(-10, 'Decimal32(9)');

SELECT accurateCastOrNull('123', 'FixedString(2)');

SELECT accurateCastOrNull(inf, 'Int64');
SELECT accurateCastOrNull(inf, 'Int128');
SELECT accurateCastOrNull(inf, 'Int256');
SELECT accurateCastOrNull(nan, 'Int64');
SELECT accurateCastOrNull(nan, 'Int128');
SELECT accurateCastOrNull(nan, 'Int256');

SELECT accurateCastOrNull(inf, 'UInt64');
SELECT accurateCastOrNull(inf, 'UInt256');
SELECT accurateCastOrNull(nan, 'UInt64');
SELECT accurateCastOrNull(nan, 'UInt256');
