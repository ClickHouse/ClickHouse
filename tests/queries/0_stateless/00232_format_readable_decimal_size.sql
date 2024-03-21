WITH round(exp(number), 6) AS x, x > 0xFFFFFFFFFFFFFFFF ? 0xFFFFFFFFFFFFFFFF : toUInt64(x) AS y, x > 0x7FFFFFFF ? 0x7FFFFFFF : toInt32(x) AS z
SELECT formatReadableDecimalSize(x), formatReadableDecimalSize(y), formatReadableDecimalSize(z)
FROM system.numbers
LIMIT 70;
WITH round(exp(number), 6) AS x, x > 0xFFFFFFFFFFFFFFFF ? 0xFFFFFFFFFFFFFFFF : toUInt64(x) AS y, x > 0x7FFFFFFF ? 0x7FFFFFFF : toInt32(x) AS z
SELECT formatReadableDecimalSize(x, 5), formatReadableDecimalSize(y, 3), formatReadableDecimalSize(z)
FROM system.numbers
LIMIT 70;
SELECT arrayJoin([3.4, Nan, null, inf]) as number, 3 as precision, formatReadableSize(number, precision);
