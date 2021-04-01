WITH round(exp(number), 6) AS x, toUInt64(x) AS y, toInt32(x) AS z
SELECT formatReadableQuantity(x), formatReadableQuantity(y), formatReadableQuantity(z)
FROM system.numbers
LIMIT 50;
