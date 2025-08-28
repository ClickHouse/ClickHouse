-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- Test all comparison operators
SELECT '=== Comparison operators ===';

SELECT 
    toDecimal512('123.456', 3) = toDecimal512('123.456', 3) AS equal,
    toDecimal512('123.456', 3) != toDecimal512('789.123', 3) AS not_equal,
    toDecimal512('123.456', 3) > toDecimal512('100.0', 1) AS greater_than,
    toDecimal512('123.456', 3) < toDecimal512('200.0', 1) AS less_than,
    toDecimal512('123.456', 3) >= toDecimal512('123.456', 3) AS greater_equal,
    toDecimal512('123.456', 3) <= toDecimal512('123.456', 3) AS less_equal;

-- Test with different scales
SELECT 
    toDecimal512('123.456', 3) = toDecimal512('123.456', 2) AS equal_different_scales,
    toDecimal512('123.456', 3) > toDecimal512('123.45', 2) AS greater_different_scales,
    toDecimal512('123.456', 3) < toDecimal512('123.46', 2) AS less_different_scales;

-- Test with zero
SELECT 
    toDecimal512('0.0', 1) = toDecimal512('0.000', 3) AS zero_equal,
    toDecimal512('123.456', 3) > toDecimal512('0.0', 1) AS positive_greater_than_zero,
    toDecimal512('-123.456', 3) < toDecimal512('0.0', 1) AS negative_less_than_zero;

-- Test with negative numbers
SELECT 
    toDecimal512('-123.456', 3) = toDecimal512('-123.456', 3) AS negative_equal,
    toDecimal512('-123.456', 3) > toDecimal512('-200.0', 1) AS negative_greater,
    toDecimal512('-123.456', 3) < toDecimal512('-100.0', 1) AS negative_less;

-- Test with very large numbers
SELECT 
    toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0) > toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999998', 0) AS large_greater,
    toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0) = toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0) AS large_equal;

-- Test BETWEEN operator
SELECT 
    toDecimal512('150.0', 1) BETWEEN toDecimal512('100.0', 1) AND toDecimal512('200.0', 1) AS between_true,
    toDecimal512('50.0', 1) BETWEEN toDecimal512('100.0', 1) AND toDecimal512('200.0', 1) AS between_false;

-- Test IN operator
SELECT 
    toDecimal512('123.456', 3) IN (toDecimal512('123.456', 3), toDecimal512('789.123', 3)) AS in_true,
    toDecimal512('999.999', 3) IN (toDecimal512('123.456', 3), toDecimal512('789.123', 3)) AS in_false;

