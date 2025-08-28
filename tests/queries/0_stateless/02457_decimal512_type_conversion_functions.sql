-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- Test conversion to other types
SELECT '=== Conversion to other types ===';

SELECT 
    toString(toDecimal512('123.456', 3)) AS to_string,
    toFloat64(toDecimal512('123.456', 3)) AS to_float64,
    toInt64(toDecimal512('123.456', 3)) AS to_int64,
    toUInt64(toDecimal512('123.456', 3)) AS to_uint64;

-- Test conversion from other types
SELECT '=== Conversion from other types ===';

SELECT 
    toDecimal512(123.456, 3) AS from_float,
    toDecimal512(123, 0) AS from_int,
    toDecimal512('123.456', 3) AS from_string;

-- Test with negative numbers
SELECT 
    toString(toDecimal512('-123.456', 3)) AS to_string_negative,
    toFloat64(toDecimal512('-123.456', 3)) AS to_float64_negative,
    toInt64(toDecimal512('-123.456', 3)) AS to_int64_negative,
    toDecimal512(-123.456, 3) AS from_float_negative,
    toDecimal512(-123, 0) AS from_int_negative,
    toDecimal512('-123.456', 3) AS from_string_negative;

-- Test with zero
SELECT 
    toString(toDecimal512('0.0', 3)) AS to_string_zero,
    toFloat64(toDecimal512('0.0', 3)) AS to_float64_zero,
    toInt64(toDecimal512('0.0', 3)) AS to_int64_zero,
    toUInt64(toDecimal512('0.0', 3)) AS to_uint64_zero,
    toDecimal512(0.0, 3) AS from_float_zero,
    toDecimal512(0, 0) AS from_int_zero,
    toDecimal512('0.0', 3) AS from_string_zero;

-- Test with very large numbers
SELECT 
    toString(toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0)) AS to_string_large,
    toFloat64(toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0)) AS to_float64_large;

-- Test with different scales
SELECT 
    toString(toDecimal512('123.456789', 6)) AS to_string_scale_6,
    toString(toDecimal512('123.456789', 3)) AS to_string_scale_3,
    toString(toDecimal512('123.456789', 0)) AS to_string_scale_0;

-- Test CAST function
SELECT '=== CAST function tests ===';

SELECT 
    CAST(toDecimal512('123.456', 3) AS String) AS cast_to_string,
    CAST(toDecimal512('123.456', 3) AS Float64) AS cast_to_float64,
    CAST(toDecimal512('123.456', 3) AS Int64) AS cast_to_int64,
    CAST(123.456 AS Decimal512(3)) AS cast_from_float64,
    CAST(123 AS Decimal512(0)) AS cast_from_int64,
    CAST('123.456' AS Decimal512(3)) AS cast_from_string;

