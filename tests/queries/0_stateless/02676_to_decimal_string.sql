-- Regular types
SELECT toDecimalString(2, 77);  -- more digits required than exist
SELECT toDecimalString(2.123456, 2);  -- rounding
SELECT toDecimalString(-2, 77);  -- more digits required than exist
SELECT toDecimalString(-2.123456, 2);  -- rounding

SELECT toDecimalString(2.9876, 60);  -- more digits required than exist (took 60 as it is float by default)
SELECT toDecimalString(2.1456, 2);  -- rounding
SELECT toDecimalString(-2.9876, 60);  -- more digits required than exist
SELECT toDecimalString(-2.1456, 2);  -- rounding

-- Float32 and Float64 tests. No sense to test big float precision -- the result will be a mess anyway.
SELECT toDecimalString(64.123::Float32, 10);
SELECT toDecimalString(64.234::Float64, 10);
SELECT toDecimalString(-64.123::Float32, 10);
SELECT toDecimalString(-64.234::Float64, 10);

-- Decimals
SELECT toDecimalString(-32.345::Decimal32(3), 3);
SELECT toDecimalString(32.345::Decimal32(3), 77);  -- more digits required than exist
SELECT toDecimalString(32.456::Decimal32(3), 2);  -- rounding
SELECT toDecimalString('-64.5671232345'::Decimal64(10), 10);
SELECT toDecimalString('128.78932312332132985464'::Decimal128(20), 20);
SELECT toDecimalString('-128.78932312332132985464123123'::Decimal128(26), 20);  -- rounding
SELECT toDecimalString('128.78932312332132985464'::Decimal128(20), 77);  -- more digits required than exist
SELECT toDecimalString('128.789323123321329854641231237893231233213298546'::Decimal256(45), 10);  -- rounding
SELECT toDecimalString('-128.789323123321329854641231237893231233213298546'::Decimal256(45), 77);  -- more digits required than exist

-- Max number of decimal fractional digits is defined as 77 for Int/UInt/Decimal and 60 for Float.
-- These values shall work OK.
SELECT toDecimalString('32.32'::Float32, 61); -- {serverError CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER}
SELECT toDecimalString('64.64'::Float64, 61); -- {serverError CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER}
SELECT toDecimalString('88'::UInt8, 78); -- {serverError CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER}
SELECT toDecimalString('646464'::Int256, 78); -- {serverError CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER}
SELECT toDecimalString('-128.789323123321329854641231237893231233213298546'::Decimal256(45), 78); -- {serverError CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER}

-- wrong types: #52407 and similar
SELECT toDecimalString('256.256'::Decimal256(45), *); -- {serverError ILLEGAL_COLUMN}
SELECT toDecimalString('128.128'::Decimal128(30), 'str'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT toDecimalString('64.64'::Decimal64(10)); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT toDecimalString('64.64'::Decimal64(10), 3, 3); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}

-- Zero precision checks
SELECT toDecimalString(1, 0);
SELECT toDecimalString(1.123456, 0);  -- rounding
SELECT toDecimalString(-1, 0);
SELECT toDecimalString(-1.123456, 0);  -- rounding
