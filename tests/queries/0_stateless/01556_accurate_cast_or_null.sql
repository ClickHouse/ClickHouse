SELECT cast(-1, 'UInt8');
SELECT accurateCastOrNull(-1, 'UInt8');

SELECT cast(257, 'Int8');
SELECT accurateCastOrNull(257, 'Int8');

SELECT accurateCastOrNull('123', 'FixedString(2)');