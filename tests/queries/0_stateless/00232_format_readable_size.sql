WITH round(exp(number), 6) AS x, x > 0xFFFFFFFFFFFFFFFF ? 0xFFFFFFFFFFFFFFFF : toUInt64(x) AS y, x > 0x7FFFFFFF ? 0x7FFFFFFF : toInt32(x) AS z
SELECT FORMAT_BYTES(x), format_bytes(y), formatReadableSize(z)
FROM system.numbers
LIMIT 70;
