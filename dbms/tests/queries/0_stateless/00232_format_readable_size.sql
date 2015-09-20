SELECT exp(number) AS x, formatReadableSize(x), toUInt64(x) AS y, formatReadableSize(y), toInt32(y) AS z, formatReadableSize(z) FROM system.numbers LIMIT 70;
