-- Tags: no_asan, no_msan, no_tsan, no_ubsan

-- Test array sorting
SELECT '=== Array sorting ===';

SELECT arraySort([toDecimal512('3.3', 1), toDecimal512('1.1', 1), toDecimal512('2.2', 1)]) AS sorted;

-- Test array distinct
SELECT '=== Array distinct ===';

SELECT arrayDistinct([toDecimal512('1.1', 1), toDecimal512('2.2', 1), toDecimal512('1.1', 1)]) AS distinct;

-- Test array reverse
SELECT '=== Array reverse ===';

SELECT arrayReverse([toDecimal512('1.1', 1), toDecimal512('2.2', 1), toDecimal512('3.3', 1)]) AS reversed;

-- Test array concat
SELECT '=== Array concat ===';

SELECT arrayConcat([toDecimal512('1.1', 1), toDecimal512('2.2', 1)], [toDecimal512('3.3', 1), toDecimal512('4.4', 1)]) AS concatenated;

-- Test array slice
SELECT '=== Array slice ===';

SELECT arraySlice([toDecimal512('1.1', 1), toDecimal512('2.2', 1), toDecimal512('3.3', 1), toDecimal512('4.4', 1)], 2, 2) AS sliced;

-- Test array push/pop operations
SELECT '=== Array push/pop operations ===';

SELECT arrayPushBack([toDecimal512('1.1', 1), toDecimal512('2.2', 1)], toDecimal512('3.3', 1)) AS push_back;
SELECT arrayPushFront([toDecimal512('2.2', 1), toDecimal512('3.3', 1)], toDecimal512('1.1', 1)) AS push_front;

-- Test array resize
SELECT '=== Array resize ===';

SELECT arrayResize([toDecimal512('1.1', 1), toDecimal512('2.2', 1)], 5, toDecimal512('0.0', 1)) AS resized_larger;
SELECT arrayResize([toDecimal512('1.1', 1), toDecimal512('2.2', 1), toDecimal512('3.3', 1)], 2) AS resized_smaller;

-- Test array with different scales
SELECT '=== Array with different scales ===';

SELECT arraySort([toDecimal512('3.333', 3), toDecimal512('1.111', 3), toDecimal512('2.222', 3)]) AS sorted_scale_3;
SELECT arrayDistinct([toDecimal512('1.111', 3), toDecimal512('2.222', 3), toDecimal512('1.111', 3)]) AS distinct_scale_3;

-- Test array with negative values
SELECT '=== Array with negative values ===';

SELECT arraySort([toDecimal512('-3.3', 1), toDecimal512('1.1', 1), toDecimal512('-2.2', 1)]) AS sorted_negative;
SELECT arrayDistinct([toDecimal512('-1.1', 1), toDecimal512('2.2', 1), toDecimal512('-1.1', 1)]) AS distinct_negative;

-- Test array with zero values
SELECT '=== Array with zero values ===';

SELECT arraySort([toDecimal512('0.0', 1), toDecimal512('1.1', 1), toDecimal512('0.0', 1)]) AS sorted_with_zero;
SELECT arrayDistinct([toDecimal512('0.0', 1), toDecimal512('1.1', 1), toDecimal512('0.0', 1)]) AS distinct_with_zero;

-- Test array with very large numbers
SELECT '=== Array with very large numbers ===';

SELECT arraySort([toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.123456', 6), toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999998.123456', 6)]) AS sorted_large;

-- Test array functions with empty arrays
SELECT '=== Array functions with empty arrays ===';

SELECT arraySort([]) AS sorted_empty;
SELECT arrayDistinct([]) AS distinct_empty;
SELECT arrayReverse([]) AS reversed_empty;
SELECT arrayConcat([], []) AS concatenated_empty;
SELECT arraySlice([], 1, 1) AS sliced_empty;

