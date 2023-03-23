-- Regular types
SELECT formatDecimal(2, 2);  -- more digits required than exist
SELECT formatDecimal(2.123456, 2);  -- rounding
SELECT formatDecimal(2.1456, 2);  -- rounding
SELECT formatDecimal(2.9876, 6);  -- more digits required than exist

-- Float32 and Float64 tests
SELECT formatDecimal(64.123::Float32, 2);
SELECT formatDecimal(64.234::Float64, 2);

-- Decimals
SELECT formatDecimal(32.345::Decimal32(3), 3);
SELECT formatDecimal(32.345::Decimal32(3), 6);  -- more digits required than exist
SELECT formatDecimal(32.456::Decimal32(3), 2);  -- rounding
SELECT formatDecimal('64.5671232345'::Decimal64(10), 10);
SELECT formatDecimal('128.78932312332132985464'::Decimal128(20), 20);
SELECT formatDecimal('128.78932312332132985464123123'::Decimal128(26), 20);  -- rounding
SELECT formatDecimal('128.78932312332132985464'::Decimal128(20), 26);  -- more digits required than exist
SELECT formatDecimal('128.789323123321329854641231237893231233213298546'::Decimal256(45), 10);  -- rounding
SELECT formatDecimal('128.789323123321329854641231237893231233213298546'::Decimal256(45), 50);  -- more digits required than exist