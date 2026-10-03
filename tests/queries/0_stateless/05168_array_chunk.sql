SELECT arrayChunk([1, 2, 3, 4, 5], 2);
SELECT arrayChunk([1, 2, 3, 4], 2);
SELECT arrayChunk([1, 2, 3], 1);
SELECT arrayChunk([1, 2, 3], 10);
SELECT arrayChunk([], 2);
SELECT arrayChunk(['a', 'b', NULL], 2);

-- non-const array
SELECT arrayChunk(range(number), 2) FROM numbers(5);
-- const array, non-const size
SELECT arrayChunk([1, 2, 3, 4], number + 1) FROM numbers(4);

SELECT arrayChunk([1, 2, 3], 0); -- { serverError BAD_ARGUMENTS }
SELECT arrayChunk([1, 2, 3], -1); -- { serverError BAD_ARGUMENTS }
SELECT arrayChunk(1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayChunk([1, 2, 3], 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayChunk([1, 2, 3]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }