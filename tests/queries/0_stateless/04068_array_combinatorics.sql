SELECT '-- basic cases';
SELECT arrayPermutations([1, 2, 3]);
SELECT arrayPartialPermutations([1, 2, 3], 2);
SELECT arrayCombinations([1, 2, 3], 2);

SELECT '-- empty and boundary cases';
SELECT arrayPermutations([]);
SELECT arrayPartialPermutations([1, 2, 3], 0);
SELECT arrayCombinations([1, 2, 3], 0);
SELECT arrayPermutations([1]);
SELECT arrayPartialPermutations([1, 2, 3], 3);
SELECT arrayCombinations([1, 2, 3], 3);
SELECT arrayCombinations([], 0);
SELECT arrayPartialPermutations([], 0);

SELECT '-- negative tests';
SELECT arrayPermutations(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayPermutations([1, 2, 3], 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayPermutations(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayPartialPermutations([1, 2, 3], 4); -- { serverError BAD_ARGUMENTS }
SELECT arrayCombinations([1, 2, 3], 4); -- { serverError BAD_ARGUMENTS }
SELECT arrayPartialPermutations([1, 2, 3], -1); -- { serverError BAD_ARGUMENTS }
SELECT arrayCombinations([1, 2, 3], -1); -- { serverError BAD_ARGUMENTS }
SELECT arrayPartialPermutations([1, 2], toUInt64(9999999999999999999)); -- { serverError BAD_ARGUMENTS }
SELECT arrayCombinations([1, 2], toUInt64(9999999999999999999)); -- { serverError BAD_ARGUMENTS }
SELECT arrayPartialPermutations([1, 2, 3], toUInt128(number + 2)) FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayCombinations([1, 2, 3], toInt128(number + 2)) FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayPartialPermutations([1, 2, 3], toNullable(2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayCombinations([1, 2, 3], toNullable(2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayPartialPermutations([1, 2, 3], CAST(NULL AS Nullable(Int8))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayCombinations([1, 2, 3], CAST(NULL AS Nullable(Int8))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- duplicates and nulls';
SELECT arrayPermutations([1, 1, 2]);
SELECT arrayPartialPermutations([1, 1, 2], 2);
SELECT arrayCombinations([1, 1, 2], 2);
SELECT arrayPermutations([1, NULL, 2]);
SELECT arrayCombinations([1, NULL, 2], 2);
SELECT arrayPartialPermutations([1, NULL, 2], 2);

SELECT '-- const and non-const inputs';
SELECT arrayPermutations(materialize([1, 2, 3]));
SELECT arrayCombinations(['a', 'b', 'c'], 2);
SELECT arrayCombinations([1, 2, 3], number) FROM numbers(4);

SELECT arrayPermutations(arr)
FROM values('arr Array(UInt8)',
    ([1, 2, 3]),
    ([4, 5]));

SELECT arrayPermutations(arr)
FROM values('arr Array(UInt8)',
    ([]),
    ([1, 2]));

SELECT arrayCombinations(arr, k)
FROM values('arr Array(UInt8), k UInt8',
    ([1, 2, 3], 2),
    ([4, 5, 6, 7], 3));

SELECT arrayPartialPermutations(arr, k)
FROM values('arr Array(UInt8), k UInt8',
    ([1, 2, 3], 2),
    ([4, 5], 1));

SELECT '-- settings';
SELECT arrayCombinations([1, 2, 3], 2) SETTINGS function_range_max_elements_in_block = 5; -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT arrayPartialPermutations([1, 2, 3], 2) SETTINGS function_range_max_elements_in_block = 5; -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT arrayPermutations([1, 2, 3, 4]) SETTINGS function_range_max_elements_in_block = 10; -- { serverError TOO_LARGE_ARRAY_SIZE }

SELECT arrayCombinations([1, 2, 3], toUInt8(number + 2))
FROM numbers(2)
SETTINGS function_range_max_elements_in_block = 8, max_block_size = 1;

SELECT arrayPermutations(arr)
FROM
(
    SELECT materialize([1, 2]) AS arr
    FROM numbers(2)
)
SETTINGS function_range_max_elements_in_block = 5, max_block_size = 1;

SELECT arrayCombinations([1, 2, 3], toUInt8(number + 2))
FROM numbers(2)
SETTINGS function_range_max_elements_in_block = 8, max_block_size = 2; -- { serverError TOO_LARGE_ARRAY_SIZE }

SELECT arrayPermutations(arr)
FROM
(
    SELECT materialize([1, 2]) AS arr
    FROM numbers(2)
)
SETTINGS function_range_max_elements_in_block = 5, max_block_size = 2; -- { serverError TOO_LARGE_ARRAY_SIZE }
