-- { echo }

SELECT arrayTranspose([[1, 2, 3], [4, 5, 6]]);
SELECT arrayTranspose([[1, 2], [3, 4], [5, 6]]);
SELECT arrayTranspose([[1, 2], [3, 4]]);

-- Single row or column
SELECT arrayTranspose([[1, 2, 3]]);
SELECT arrayTranspose([[1], [2], [3]]);
SELECT arrayTranspose([[42]]);

-- Empty arrays
SELECT arrayTranspose([]::Array(Array(Int64)));
SELECT arrayTranspose([[]]);
SELECT arrayTranspose([[], []]);

-- Floating point
SELECT arrayTranspose([[1.5, 2.5], [3.5, 4.5]]);

-- Large integer types
SELECT arrayTranspose([[toUInt128(1), toUInt128(2)], [toUInt128(3), toUInt128(4)]]);

-- UUID
SELECT arrayTranspose([[toUUID('00000000-0000-0000-0000-000000000001'), toUUID('00000000-0000-0000-0000-000000000002')], [toUUID('00000000-0000-0000-0000-000000000003'), toUUID('00000000-0000-0000-0000-000000000004')]]);

-- Decimal
SELECT arrayTranspose([[toDecimal32(1.5, 1), toDecimal32(2.5, 1)], [toDecimal32(3.5, 1), toDecimal32(4.5, 1)]]);

-- FixedString
SELECT arrayTranspose([[toFixedString('ab', 2), toFixedString('cd', 2)], [toFixedString('ef', 2), toFixedString('gh', 2)]]);

-- Strings
SELECT arrayTranspose([['a', 'b', 'c'], ['d', 'e', 'f']]);

-- Nullable elements
SELECT arrayTranspose([[1, NULL, 3], [4, 5, NULL]]);
SELECT arrayTranspose([[toNullable('a'), NULL], [toNullable('b'), toNullable('c')]]);

-- Multiple rows
SELECT arrayTranspose(a) FROM (
    SELECT [[1, 2], [3, 4]] AS a
    UNION ALL
    SELECT [[5, 6, 7], [8, 9, 10], [11, 12, 13]] AS a
) ORDER BY a;

-- Matrix of arrays
SELECT arrayTranspose([[[1, 2], []], [[3], [4, 5, 6]]]);

-- Tuple elements
SELECT arrayTranspose([[tuple(1, 'a'), tuple(2, 'b')], [tuple(3, 'c'), tuple(4, 'd')]]);

-- Double transpose returns the initial input
SELECT arrayTranspose(arrayTranspose([[1, 2, 3], [4, 5, 6]]));

-- Large matrices to check cache-oblivious recursion (both row-split and column-split branches)
WITH arrayMap(i -> arrayMap(j -> toUInt64(i * 100 + j), range(128)), range(128)) AS mat
SELECT arrayTranspose(mat) = arrayMap(j -> arrayMap(i -> toUInt64(i * 100 + j), range(128)), range(128));

WITH arrayMap(i -> arrayMap(j -> toFixedString(concat('a_', toString(i), '_', toString(j)), 9), range(128)), range(128)) AS mat
SELECT arrayTranspose(mat) = arrayMap(j -> arrayMap(i -> toFixedString(concat('a_', toString(i), '_', toString(j)), 9), range(128)), range(128));

-- Error: inner arrays of different sizes
SELECT arrayTranspose([[1, 2], [3]]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

-- Error: not a 2D array
SELECT arrayTranspose([1, 2, 3]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
