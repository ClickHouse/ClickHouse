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

-- size limit: should succeed (C(10,3)=120, total elements = 120*3 = 360 < 1M)
SELECT length(arrayCombinations(range(toUInt8(10)), 3));

-- size limit: should fail — C(20,10)=184756 rows but 184756*10 = 1847560 total elements > 1M
SELECT arrayCombinations(range(toUInt8(20)), 10); -- {serverError TOO_LARGE_ARRAY_SIZE}

-- size limit: should fail (15! > 1M)
SELECT arrayPermutations(range(toUInt8(15))); -- {serverError TOO_LARGE_ARRAY_SIZE}

-- size limit: should fail — P(9,9)=362880 rows but 362880*9 = 3265920 total elements > 1M
SELECT arrayPermutations(range(toUInt8(9))); -- {serverError TOO_LARGE_ARRAY_SIZE}
