WITH round(exp(number), 6) AS x, x > 0xFFFFFFFFFFFFFFFF ? 0xFFFFFFFFFFFFFFFF : toUInt64(x) AS y, x > 0x7FFFFFFF ? 0x7FFFFFFF : toInt32(x) AS z
SELECT FORMAT_BYTES(x), format_bytes(y), formatReadableSize(z)
FROM system.numbers
LIMIT 70;
WITH round(exp(number), 6) AS x, x > 0xFFFFFFFFFFFFFFFF ? 0xFFFFFFFFFFFFFFFF : toUInt64(x) AS y, x > 0x7FFFFFFF ? 0x7FFFFFFF : toInt32(x) AS z
SELECT FORMAT_BYTES(x), format_bytes(y, 4), formatReadableSize(z, 8)
FROM system.numbers
LIMIT 70;
WITH round(exp(number), 6) AS x, x > 0xFFFFFFFFFFFFFFFF ? 0xFFFFFFFFFFFFFFFF : toUInt64(x) AS y, x > 0x7FFFFFFF ? 0x7FFFFFFF : toInt32(x) AS z
SELECT FORMAT_BYTES(x, 100), format_bytes(y, 100), formatReadableSize(z, 100)
FROM system.numbers
LIMIT 70; -- { serverError 28 }
WITH round(exp(number), 6) AS x, x > 0xFFFFFFFFFFFFFFFF ? 0xFFFFFFFFFFFFFFFF : toUInt64(x) AS y, x > 0x7FFFFFFF ? 0x7FFFFFFF : toInt32(x) AS z
SELECT FORMAT_BYTES(x, 256), format_bytes(y, 256), formatReadableSize(z, 256)
FROM system.numbers
LIMIT 70; -- { serverError 43 }
