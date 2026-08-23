-- arrayPackBits*: one bit per element, packed most-significant-bit first within each byte.
-- The UInt64 variants interpret the packed byte stream in little-endian byte order (issue #48830).
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

-- The UInt64 result equals the little-endian reinterpretation of the byte stream of the String variant.
SELECT arrayPackBitsToUInt64(x -> x, [1, 0, 1, 1, 0, 0, 1, 0, 1]) = reinterpretAsUInt64(arrayPackBitsToString(x -> x, [1, 0, 1, 1, 0, 0, 1, 0, 1]));
SELECT arrayPackBitGroupsToUInt64(x -> x, 4, [1, 2, 3]) = reinterpretAsUInt64(arrayPackBitGroupsToString(x -> x, 4, [1, 2, 3]));
SELECT arrayPackBitGroupsToUInt64(x -> x, 64, [12345678901234567890]) = reinterpretAsUInt64(arrayPackBitGroupsToString(x -> x, 64, [12345678901234567890]));

-- Wide-integer lambda results are supported: the bit (getBool) and group (getUInt) values are read from the full
-- value, so a wide result behaves like its native counterpart and a group keeps its low `g` bits.
SELECT arrayPackBitsToUInt64(x -> toUInt128(x), [1, 0, 0, 0, 0, 0]);
SELECT arrayPackBitsToUInt64(x -> x, [toUInt128('18446744073709551616'), 0, 0, 0, 0, 0, 0, 0]); -- 2^64 is truthy even though its low 64 bits are zero
SELECT arrayPackBitGroupsToUInt64(x -> toUInt256(x), 4, [1, 2, 3]);

-- invalid fixed parameters are rejected.
SELECT arrayPackBitsToFixedString(x -> x, 0, [1]); -- { serverError BAD_ARGUMENTS }
SELECT arrayPackBitGroupsToUInt64(x -> x, 0, [1]); -- { serverError BAD_ARGUMENTS }
SELECT arrayPackBitGroupsToUInt64(x -> x, -1, [1]); -- { serverError BAD_ARGUMENTS }
SELECT arrayPackBitGroupsToUInt64(x -> x, 65, [1]); -- { serverError BAD_ARGUMENTS }

-- The fixed size/group arguments must be native integers, so a wide constant cannot be silently truncated to its low
-- 64 bits. toUInt128('18446744073709551620') is 2^64 + 4 and would become the group size 4 if read through a 64-bit
-- accessor; toUInt128('18446744073709551617') is 2^64 + 1 and would become the FixedString size 1.
SELECT arrayPackBitGroupsToUInt64(x -> x, toUInt128('18446744073709551620'), [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayPackBitGroupsToUInt64(x -> x, toUInt256('18446744073709551620'), [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayPackBitsToFixedString(x -> x, toUInt128('18446744073709551617'), [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayPackBitGroupsToFixedString(x -> x, toUInt256('18446744073709551617'), 4, [3]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- the lambda must return an integer; Decimal/Float results are rejected during analysis.
SELECT arrayPackBitsToUInt64(x -> toFloat64(x), [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayPackBitGroupsToUInt64(x -> toDecimal64(x, 0), 4, [15]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Like the other higher-order array functions, several arrays of equal size can be passed to the lambda.
SELECT arrayPackBitsToUInt64((x, y) -> x > y, [1, 0, 1, 0, 0, 0], [0, 1, 0, 1, 1, 1]);
SELECT hex(arrayPackBitsToString((x, y) -> x > y, [1, 0, 1, 0, 0, 0, 0, 0, 1], [0, 1, 0, 1, 1, 1, 1, 1, 0]));
SELECT hex(arrayPackBitsToFixedString((x, y) -> x > y, 2, [1, 0, 1, 0, 0, 0, 0, 0, 1], [0, 1, 0, 1, 1, 1, 1, 1, 0]));
SELECT arrayPackBitGroupsToUInt64((x, y) -> x + y, 4, [1, 2, 3], [0, 0, 0]);
SELECT hex(arrayPackBitGroupsToString((x, y) -> x + y, 4, [1, 2, 3], [0, 0, 0]));
SELECT hex(arrayPackBitGroupsToFixedString((x, y) -> x + y, 4, 4, [1, 2, 3], [0, 0, 0]));
SELECT arrayPackBitsToUInt64((x, y, z) -> x + y + z, [1, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]);

-- arrays of different sizes are rejected, as for the other higher-order array functions.
SELECT arrayPackBitsToUInt64((x, y) -> x > y, [1, 0], [0]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
