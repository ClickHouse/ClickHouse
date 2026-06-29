-- arrayPackBits*: one bit per element, packed most-significant-bit first.
SELECT arrayPackBitsToUInt64(x -> x, [1, 0, 0, 0, 0, 0]);
SELECT arrayPackBitsToUInt64(x -> x, [1, 0, 0, 0, 0, 1]);
SELECT arrayPackBitsToUInt64(x -> x, [1, 0, 0, 0, 0, 0, 0, 0, 0]);
SELECT arrayPackBitsToUInt64(x -> x, [1, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
SELECT arrayPackBitsToUInt64(x -> x, [1, 0, 0, 0, 0, 0 ,0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 ,0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 ,0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 ,0, 0, 0, 0, 0, 0, 0, 0, 0 ,0, 0, 0, 0, 0, 0, 0, 0, 0 ,0, 0, 0, 0, 0, 0, 0, 0, 0 ,0, 0, 0, 0, 0, 0]);

SELECT arrayPackBitsToString(x -> x, [0, 0, 1, 1, 0, 0, 0, 0]);
SELECT arrayPackBitsToString(x -> x, [0, 0, 1, 1, 0, 0, 0, 0,0, 0, 1, 1, 0, 0, 0, 1]);

SELECT arrayPackBitsToFixedString(x -> x, 1, [0, 0, 1, 1, 0, 0, 0, 0]);
SELECT arrayPackBitsToFixedString(x -> x, 2, [0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1]);
SELECT arrayPackBitsToFixedString(x -> x, 1, [0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1]);

-- arrayPackBitGroups*: the lambda returns a number whose low g bits form a group; groups are packed contiguously.
SELECT arrayPackBitGroupsToUInt64(x -> x, 4, [15, 0]);
SELECT arrayPackBitGroupsToUInt64(x -> x, 4, [1, 2, 3]);
SELECT arrayPackBitGroupsToUInt64(x -> x, 1, [1, 1]);
SELECT arrayPackBitGroupsToUInt64(x -> x, 8, [255, 1]);
SELECT arrayPackBitGroupsToUInt64(x -> x, 2, [7, 1]); -- only the low 2 bits of each value are kept
SELECT arrayPackBitGroupsToUInt64(x -> x * 5, 4, [1, 2, 3]); -- multi-bit lambda values 5, 10, 15
SELECT arrayPackBitGroupsToUInt64(x -> x, 4, [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]); -- only the first 16 groups fit

SELECT arrayPackBitGroupsToString(x -> x, 4, [3, 0, 3, 1]);
SELECT arrayPackBitGroupsToString(x -> x, 2, [0, 3, 0, 0]);
SELECT arrayPackBitGroupsToString(x -> x, 4, [3, 0, 3]); -- a trailing partial byte is left-aligned

SELECT arrayPackBitGroupsToFixedString(x -> x, 2, 4, [3, 0, 3, 1]);
SELECT arrayPackBitGroupsToFixedString(x -> x, 1, 4, [3, 0, 3, 1]); -- only the first 2 groups fit into 1 byte
SELECT hex(arrayPackBitGroupsToFixedString(x -> x, 2, 4, [3, 0])); -- zero-padded to 2 bytes
SELECT length(arrayPackBitGroupsToFixedString(x -> x, 2, 4, [3, 0]));

-- invalid fixed parameters are rejected.
SELECT arrayPackBitsToFixedString(x -> x, 0, [1]); -- { serverError BAD_ARGUMENTS }
SELECT arrayPackBitGroupsToUInt64(x -> x, 0, [1]); -- { serverError BAD_ARGUMENTS }
SELECT arrayPackBitGroupsToUInt64(x -> x, -1, [1]); -- { serverError BAD_ARGUMENTS }
SELECT arrayPackBitGroupsToUInt64(x -> x, 65, [1]); -- { serverError BAD_ARGUMENTS }

-- the lambda must return an integer; Decimal/Float results are rejected during analysis.
SELECT arrayPackBitsToUInt64(x -> toFloat64(x), [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayPackBitGroupsToUInt64(x -> toDecimal64(x, 0), 4, [15]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
