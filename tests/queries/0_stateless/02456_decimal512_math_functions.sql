-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- Test mathematical functions
SELECT '=== Mathematical functions ===';

SELECT 
    abs(toDecimal512('-123.456', 3)) AS absolute,
    round(toDecimal512('123.456789', 6), 2) AS rounded,
    floor(toDecimal512('123.456789', 6)) AS floored,
    ceil(toDecimal512('123.456789', 6)) AS ceiled,
    trunc(toDecimal512('123.456789', 6), 2) AS truncated;

-- Test with negative numbers
SELECT 
    abs(toDecimal512('-999.999', 3)) AS absolute_negative,
    round(toDecimal512('-123.456789', 6), 2) AS rounded_negative,
    floor(toDecimal512('-123.456789', 6)) AS floored_negative,
    ceil(toDecimal512('-123.456789', 6)) AS ceiled_negative,
    trunc(toDecimal512('-123.456789', 6), 2) AS truncated_negative;

-- Test with zero
SELECT 
    abs(toDecimal512('0.0', 3)) AS absolute_zero,
    round(toDecimal512('0.0', 6), 2) AS rounded_zero,
    floor(toDecimal512('0.0', 6)) AS floored_zero,
    ceil(toDecimal512('0.0', 6)) AS ceiled_zero,
    trunc(toDecimal512('0.0', 6), 2) AS truncated_zero;

-- Test with very large numbers
SELECT 
    abs(toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0)) AS absolute_large,
    round(toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.123456789', 9), 3) AS rounded_large;

-- Test with different scales
SELECT 
    round(toDecimal512('123.456789', 6), 0) AS rounded_to_integer,
    round(toDecimal512('123.456789', 6), 4) AS rounded_to_4_decimals,
    round(toDecimal512('123.456789', 6), 6) AS rounded_to_6_decimals,
    round(toDecimal512('123.456789', 6), 8) AS rounded_to_8_decimals;

-- Test trunc with different scales
SELECT 
    trunc(toDecimal512('123.456789', 6), 0) AS trunc_to_integer,
    trunc(toDecimal512('123.456789', 6), 2) AS trunc_to_2_decimals,
    trunc(toDecimal512('123.456789', 6), 4) AS trunc_to_4_decimals,
    trunc(toDecimal512('123.456789', 6), 6) AS trunc_to_6_decimals;

