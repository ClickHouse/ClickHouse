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

SELECT arrayRemoveAt([1, 2, 3], toUInt64(18446744073709551615));
SELECT toTypeName(arrayRemoveAt(CAST([1, 2, 3], 'Array(UInt8)'), toInt8(2)));

SELECT arrayRemoveAt([1, 2, 3], 0); -- { serverError ZERO_ARRAY_OR_TUPLE_INDEX }
SELECT arrayRemoveAt(1, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayRemoveAt([1, 2, 3], 1.5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
