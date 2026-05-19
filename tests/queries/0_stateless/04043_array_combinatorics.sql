-- arrayCombinations
SELECT arrayCombinations([1, 2, 3], 2);
SELECT arrayCombinations([1, 2, 3, 4], 3);
SELECT arrayCombinations(['a', 'b', 'c'], 1);
SELECT arrayCombinations([1, 2, 3], 0);
SELECT arrayCombinations([1, 2, 3], 3);

-- arrayPermutations
SELECT arrayPermutations([1, 2, 3]);
SELECT arrayPermutations([1]);
SELECT arrayPermutations(emptyArrayUInt8());

-- arrayPartialPermutations
SELECT arrayPartialPermutations([1, 2, 3], 2);
SELECT arrayPartialPermutations([1, 2, 3], 1);
SELECT arrayPartialPermutations([1, 2, 3], 0);

-- error cases
SELECT arrayCombinations([1, 2], 3); -- {serverError BAD_ARGUMENTS}
SELECT arrayCombinations([1, 2], -1); -- {serverError BAD_ARGUMENTS}
SELECT arrayPartialPermutations([1, 2], 3); -- {serverError BAD_ARGUMENTS}
SELECT arrayPartialPermutations([1, 2], -1); -- {serverError BAD_ARGUMENTS}

-- multi-row inputs with varying array length, to exercise per-row offset transitions
SELECT arrayCombinations(arr, 2) FROM (SELECT [1, 2, 3] AS arr UNION ALL SELECT [10, 20] UNION ALL SELECT [100, 200, 300, 400]) ORDER BY arr;
SELECT arrayPermutations(arr) FROM (SELECT [1, 2] AS arr UNION ALL SELECT emptyArrayUInt8() UNION ALL SELECT [7, 8, 9]) ORDER BY arr;
SELECT arrayPartialPermutations(arr, 2) FROM (SELECT [1, 2, 3] AS arr UNION ALL SELECT [4, 5] UNION ALL SELECT [9, 8, 7]) ORDER BY arr;

-- size limit: should succeed (C(10,3)=120, total elements = 120*3 = 360 < 1M)
SELECT length(arrayCombinations(range(toUInt8(10)), 3));

-- size limit: should fail — C(20,10)=184756 rows but 184756*10 = 1847560 total elements > 1M
SELECT arrayCombinations(range(toUInt8(20)), 10); -- {serverError TOO_LARGE_ARRAY_SIZE}

-- size limit: k == n fast path must honor the element cap.
-- C(n,n)=1 but the single combination has n elements, which exceeds the 1M cap when n > 1M.
SELECT arrayCombinations(range(1000001), 1000001); -- {serverError TOO_LARGE_ARRAY_SIZE}

-- size limit: should fail (15! > 1M)
SELECT arrayPermutations(range(toUInt8(15))); -- {serverError TOO_LARGE_ARRAY_SIZE}

-- size limit: should fail — P(9,9)=362880 rows but 362880*9 = 3265920 total elements > 1M
SELECT arrayPermutations(range(toUInt8(9))); -- {serverError TOO_LARGE_ARRAY_SIZE}
