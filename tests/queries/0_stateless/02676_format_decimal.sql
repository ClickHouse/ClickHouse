SELECT formatDecimal(2,2); -- 2.00
SELECT formatDecimal(2.123456,2); -- 2.12
SELECT formatDecimal(2.1456,2); -- 2.15 -- rounding!
SELECT formatDecimal(64.32::Float64, 2); -- 64.32
SELECT formatDecimal(64.32::Decimal32(2), 2); -- 64.32
SELECT formatDecimal(64.32::Decimal64(2), 2); -- 64.32