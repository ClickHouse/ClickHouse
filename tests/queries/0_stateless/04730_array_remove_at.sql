SELECT arrayRemoveAt([1, 2, 3, 4], 1);
SELECT arrayRemoveAt([1, 2, 3, 4], 2);
SELECT arrayRemoveAt([1, 2, 3, 4], 4);

SELECT arrayRemoveAt([1, 2, 3, 4], -1);
SELECT arrayRemoveAt([1, 2, 3, 4], -2);
SELECT arrayRemoveAt([1, 2, 3, 4], -4);

SELECT arrayRemoveAt([1, 2, 3, 4], 5);
SELECT arrayRemoveAt([1, 2, 3, 4], -5);
SELECT arrayRemoveAt([], 1);

SELECT arrayRemoveAt(['a', 'b', 'c'], 2);
SELECT arrayRemoveAt(CAST([1, NULL, 3], 'Array(Nullable(Int8))'), 2);

SELECT
    number,
    arrayRemoveAt(
        [number, number + 1, number + 2],
        if(number % 2 = 0, toInt8(1), toInt8(-1)))
FROM numbers(4)
ORDER BY number;

-- Constant array with a dynamic index must stay compact instead of being materialized per row.
SELECT
    number,
    arrayRemoveAt(
        [10, 20, 30, 40],
        multiIf(number = 0, toInt8(1), number = 1, toInt8(-1), number = 2, toInt8(2), toInt8(10)))
FROM numbers(4)
ORDER BY number;

-- Constant index is read once. A position larger than the whole nested column is guaranteed out of bounds.
SELECT number, arrayRemoveAt([number, number + 1, number + 2], toInt8(2)) FROM numbers(3) ORDER BY number;
SELECT number, arrayRemoveAt([number, number + 1], toUInt64(18446744073709551615)) FROM numbers(3) ORDER BY number;

SELECT arrayRemoveAt([1, 2, 3], toUInt64(18446744073709551615));
SELECT arrayRemoveAt([1, 2, 3], toInt64(-9223372036854775807) - 1);
SELECT toTypeName(arrayRemoveAt(CAST([1, 2, 3], 'Array(UInt8)'), toInt8(2)));
SELECT arrayRemoveAt([[1, 2], [3, 4], [5, 6]], 2);

SELECT
    number,
    arrayRemoveAt(
        arrayMap(x -> toUInt64(number + x), range(number % 5)),
        multiIf(number = 0, toInt8(1), number = 1, toInt8(1), number = 2, toInt8(9), number = 3, toInt8(-1), toInt8(2)))
FROM numbers(5)
ORDER BY number;

SELECT number, arrayRemoveAt([number, number + 1], toInt8(10 + number)) FROM numbers(3) ORDER BY number;
SELECT number, arrayRemoveAt(CAST([toUInt64(number), NULL, toUInt64(number + 2)], 'Array(Nullable(UInt64))'), toInt8(2)) FROM numbers(3) ORDER BY number;

-- Dynamic unsigned indexes use the same positional semantics as signed indexes.
SELECT number, arrayRemoveAt([number, number + 1], toUInt8(number % 2 + 1)) FROM numbers(3) ORDER BY number;

-- Singleton arrays exercise the lazy no-op path for dynamic arrays.
SELECT number, arrayRemoveAt([number], toInt8(2)) FROM numbers(3) ORDER BY number;

-- A dynamic index that is out of bounds for every row keeps a constant array compact.
SELECT number, arrayRemoveAt([1], toInt8(number + 2)) FROM numbers(3) ORDER BY number;

SELECT arrayRemoveAt([1, 2, 3], 0); -- { serverError ZERO_ARRAY_OR_TUPLE_INDEX }
SELECT arrayRemoveAt([1, 2, 3], if(number = 0, toInt8(0), toInt8(1))) FROM numbers(2); -- { serverError ZERO_ARRAY_OR_TUPLE_INDEX }
SELECT arrayRemoveAt(1, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayRemoveAt([1, 2, 3], 1.5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayRemoveAt([1, 2, 3], toNullable(toInt8(2))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
