-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- Test array creation and operations
SELECT '=== Array creation and operations ===';

SELECT [toDecimal512('1.1', 1), toDecimal512('2.2', 1), toDecimal512('3.3', 1)] AS decimal_array;

-- Test array aggregation functions
SELECT '=== Array aggregation functions ===';

SELECT 
    arraySum([toDecimal512('1.1', 1), toDecimal512('2.2', 1), toDecimal512('3.3', 1)]) AS sum,
    arrayAvg([toDecimal512('1.1', 1), toDecimal512('2.2', 1), toDecimal512('3.3', 1)]) AS avg,
    arrayMin([toDecimal512('1.1', 1), toDecimal512('2.2', 1), toDecimal512('3.3', 1)]) AS min,
    arrayMax([toDecimal512('1.1', 1), toDecimal512('2.2', 1), toDecimal512('3.3', 1)]) AS max,
    arrayProduct([toDecimal512('2.0', 1), toDecimal512('3.0', 1), toDecimal512('4.0', 1)]) AS product;

-- Test with different scales
SELECT 
    arraySum([toDecimal512('1.123', 3), toDecimal512('2.234', 3), toDecimal512('3.345', 3)]) AS sum_scale_3,
    arrayAvg([toDecimal512('1.123', 3), toDecimal512('2.234', 3), toDecimal512('3.345', 3)]) AS avg_scale_3,
    arrayMin([toDecimal512('1.123', 3), toDecimal512('2.234', 3), toDecimal512('3.345', 3)]) AS min_scale_3,
    arrayMax([toDecimal512('1.123', 3), toDecimal512('2.234', 3), toDecimal512('3.345', 3)]) AS max_scale_3;

-- Test with negative values
SELECT 
    arraySum([toDecimal512('-1.1', 1), toDecimal512('2.2', 1), toDecimal512('-3.3', 1)]) AS sum_negative,
    arrayAvg([toDecimal512('-1.1', 1), toDecimal512('2.2', 1), toDecimal512('-3.3', 1)]) AS avg_negative,
    arrayMin([toDecimal512('-1.1', 1), toDecimal512('2.2', 1), toDecimal512('-3.3', 1)]) AS min_negative,
    arrayMax([toDecimal512('-1.1', 1), toDecimal512('2.2', 1), toDecimal512('-3.3', 1)]) AS max_negative;

-- Test with zero values
SELECT 
    arraySum([toDecimal512('0.0', 1), toDecimal512('1.1', 1), toDecimal512('0.0', 1)]) AS sum_with_zero,
    arrayAvg([toDecimal512('0.0', 1), toDecimal512('1.1', 1), toDecimal512('0.0', 1)]) AS avg_with_zero,
    arrayMin([toDecimal512('0.0', 1), toDecimal512('1.1', 1), toDecimal512('0.0', 1)]) AS min_with_zero,
    arrayMax([toDecimal512('0.0', 1), toDecimal512('1.1', 1), toDecimal512('0.0', 1)]) AS max_with_zero;

-- Test with single element
SELECT 
    arraySum([toDecimal512('999.999', 3)]) AS sum_single,
    arrayAvg([toDecimal512('999.999', 3)]) AS avg_single,
    arrayMin([toDecimal512('999.999', 3)]) AS min_single,
    arrayMax([toDecimal512('999.999', 3)]) AS max_single,
    arrayProduct([toDecimal512('999.999', 3)]) AS product_single;

-- Test with empty array
SELECT 
    arraySum([]) AS sum_empty,
    arrayAvg([]) AS avg_empty,
    arrayMin([]) AS min_empty,
    arrayMax([]) AS max_empty,
    arrayProduct([]) AS product_empty;

-- Test with very large numbers
SELECT 
    arraySum([toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.123456', 6)]) AS sum_large,
    arrayAvg([toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.123456', 6)]) AS avg_large;

-- Test array element access
SELECT 
    [toDecimal512('1.1', 1), toDecimal512('2.2', 1), toDecimal512('3.3', 1)][1] AS element_1,
    [toDecimal512('1.1', 1), toDecimal512('2.2', 1), toDecimal512('3.3', 1)][2] AS element_2,
    [toDecimal512('1.1', 1), toDecimal512('2.2', 1), toDecimal512('3.3', 1)][3] AS element_3;

-- Test array length
SELECT 
    length([toDecimal512('1.1', 1), toDecimal512('2.2', 1), toDecimal512('3.3', 1)]) AS array_length,
    length([]) AS empty_array_length;

