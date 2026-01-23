-- FIXME
-- Tags: xfail
WITH (SELECT dummy AS x, concatWithSeparator('a', 1, 1, 1, 1) FROM system.one QUALIFY cityHash64(_CAST('World', 'Variant(FixedString(14), Float32, Float64, IPv6, Map(UInt8, Int8), UUID)'), isNotNull(isNullable(1)), 'limit', concatWithSeparator('a', '|', concatWithSeparator('a', 1, 1, '|')))) AS y SELECT DISTINCT concatWithSeparator('a', 1, 1), y FROM remote('127.0.0.{1,2}', system.one) GROUP BY y WITH TOTALS
