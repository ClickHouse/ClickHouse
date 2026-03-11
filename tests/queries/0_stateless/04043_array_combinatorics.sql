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
