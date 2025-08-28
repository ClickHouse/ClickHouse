-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- Test basic arithmetic operations
SELECT '=== Basic arithmetic operations ===';

SELECT 
    toDecimal512('123.456', 3) + toDecimal512('789.123', 3) AS addition,
    toDecimal512('123.456', 3) - toDecimal512('789.123', 3) AS subtraction,
    toDecimal512('123.456', 3) * toDecimal512('789.123', 3) AS multiplication,
    toDecimal512('123.456', 3) / toDecimal512('789.123', 3) AS division;

-- Test modulo operation
SELECT toDecimal512('123.456', 3) % toDecimal512('10.0', 1) AS modulo;

-- Test unary minus
SELECT -toDecimal512('123.456', 3) AS unary_minus;

-- Test with different scales
SELECT 
    toDecimal512('123.456', 3) + toDecimal512('789.123', 2) AS addition_different_scales,
    toDecimal512('123.456', 3) - toDecimal512('789.123', 2) AS subtraction_different_scales,
    toDecimal512('123.456', 3) * toDecimal512('789.123', 2) AS multiplication_different_scales,
    toDecimal512('123.456', 3) / toDecimal512('789.123', 2) AS division_different_scales;

-- Test with zero
SELECT 
    toDecimal512('123.456', 3) + toDecimal512('0.0', 1) AS addition_with_zero,
    toDecimal512('123.456', 3) - toDecimal512('0.0', 1) AS subtraction_with_zero,
    toDecimal512('123.456', 3) * toDecimal512('0.0', 1) AS multiplication_with_zero;

-- Test division by zero (should throw error)
SELECT toDecimal512('123.456', 3) / toDecimal512('0.0', 1); -- { serverError ILLEGAL_DIVISION }

-- Test large numbers
SELECT 
    toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0) AS max_value,
    toDecimal512('1', 0) AS one,
    toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0) + toDecimal512('1', 0) AS max_plus_one;
