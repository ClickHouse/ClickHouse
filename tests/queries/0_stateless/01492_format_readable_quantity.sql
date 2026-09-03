WITH round(exp(number), 6) AS x, toUInt64(x) AS y, toInt32(min2(x, 2147483647)) AS z
SELECT formatReadableQuantity(x), formatReadableQuantity(y), formatReadableQuantity(z)
FROM system.numbers
LIMIT 45;
