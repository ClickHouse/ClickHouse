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

SELECT arrayPackBitGroupsToUInt64(x -> x, 2, [1, 1]);
SELECT arrayPackBitGroupsToUInt64(x -> x, 1, [1, 1]);
SELECT arrayPackBitGroupsToUInt64(x -> x, 2, [1, 0, 0]);

SELECT arrayPackBitGroupsToString(x -> x, 6, [1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0]);
SELECT arrayPackBitGroupsToString(x -> x, 6, [1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0]);

SELECT toString(arrayPackBitGroupsToFixedString(x -> x, 2, 6, [1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0]));
SELECT toString(arrayPackBitGroupsToFixedString(x -> x, 1, 8, [0, 0, 1, 1, 0, 0, 0, 0]));
SELECT toString(arrayPackBitGroupsToFixedString(x -> x, 1, 6, [1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0]));

SELECT toString(arrayPackBitGroupsToFixedString(x -> x, 2, 6, [1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0]));
SELECT toString(arrayPackBitGroupsToFixedString(x -> x, 3, 6, [1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0]));
SELECT toString(arrayPackBitGroupsToFixedString(x -> x, 8, 6, [1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0]));
SELECT length(arrayPackBitGroupsToFixedString(x -> x, 2, 6, [1, 1, 0, 0, 0, 1]));

