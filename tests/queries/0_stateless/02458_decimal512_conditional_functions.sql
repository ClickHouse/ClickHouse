-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- Test conditional functions
SELECT '=== Conditional functions ===';

SELECT 
    if(toDecimal512('123.456', 3) > toDecimal512('100.0', 1), 'high', 'low') AS simple_if,
    multiIf(
        toDecimal512('123.456', 3) > toDecimal512('200.0', 1), 'very_high',
        toDecimal512('123.456', 3) > toDecimal512('100.0', 1), 'high',
        'low'
    ) AS multi_if;

-- Test with different conditions
SELECT 
    if(toDecimal512('123.456', 3) = toDecimal512('123.456', 3), 'equal', 'not_equal') AS if_equal,
    if(toDecimal512('123.456', 3) != toDecimal512('789.123', 3), 'not_equal', 'equal') AS if_not_equal,
    if(toDecimal512('123.456', 3) > toDecimal512('100.0', 1), 'greater', 'not_greater') AS if_greater,
    if(toDecimal512('123.456', 3) < toDecimal512('200.0', 1), 'less', 'not_less') AS if_less;

-- Test with zero
SELECT 
    if(toDecimal512('123.456', 3) > toDecimal512('0.0', 1), 'positive', 'not_positive') AS if_positive,
    if(toDecimal512('-123.456', 3) < toDecimal512('0.0', 1), 'negative', 'not_negative') AS if_negative,
    if(toDecimal512('0.0', 3) = toDecimal512('0.0', 1), 'zero', 'not_zero') AS if_zero;

-- Test multiIf with more conditions
SELECT 
    multiIf(
        toDecimal512('123.456', 3) > toDecimal512('200.0', 1), 'very_high',
        toDecimal512('123.456', 3) > toDecimal512('100.0', 1), 'high',
        toDecimal512('123.456', 3) > toDecimal512('50.0', 1), 'medium',
        toDecimal512('123.456', 3) > toDecimal512('0.0', 1), 'low',
        'negative'
    ) AS multi_if_extended;

-- Test with different scales
SELECT 
    if(toDecimal512('123.456', 3) > toDecimal512('123.45', 2), 'greater_different_scales', 'not_greater') AS if_different_scales,
    multiIf(
        toDecimal512('123.456', 3) > toDecimal512('123.46', 2), 'greater',
        toDecimal512('123.456', 3) = toDecimal512('123.45', 2), 'equal',
        'less'
    ) AS multi_if_different_scales;

-- Test with very large numbers
SELECT 
    if(toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0) > toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999998', 0), 'max_greater', 'max_not_greater') AS if_large_numbers;

-- Test with negative numbers
SELECT 
    if(toDecimal512('-123.456', 3) > toDecimal512('-200.0', 1), 'negative_greater', 'negative_not_greater') AS if_negative_greater,
    multiIf(
        toDecimal512('-123.456', 3) > toDecimal512('0.0', 1), 'positive',
        toDecimal512('-123.456', 3) = toDecimal512('0.0', 1), 'zero',
        toDecimal512('-123.456', 3) > toDecimal512('-100.0', 1), 'slightly_negative',
        'very_negative'
    ) AS multi_if_negative;

