-- Regression test for UBSan crash in positiveModulo when b exceeds the signed OriginResultType range.
-- https://github.com/ClickHouse/ClickHouse/issues/100510
SELECT positiveModulo(toInt64(-9000000000000000000), toUInt64(10000000000000000000));
SELECT positiveModulo(toInt64(-1), toUInt64(18446744073709551615));
SELECT positiveModulo(toInt8(-1), toUInt64(10000000000000000000));
