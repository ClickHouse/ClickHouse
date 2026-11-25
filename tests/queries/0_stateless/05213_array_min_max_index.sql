-- { echoOn }

-- Basic behavior and first ties.
SELECT arrayMinIndex([5, 3, 2, 7]), arrayMaxIndex([5, 3, 2, 7]);
SELECT arrayMinIndex([5, 3, 3, 7]), arrayMaxIndex([5, 7, 7, 3]);
SELECT arrayMinIndex(emptyArrayInt32()), arrayMaxIndex(emptyArrayUInt64());
SELECT arrayMinIndex([42]), arrayMaxIndex([42]);

-- Lambda forms.
SELECT arrayMinIndex(x -> abs(x), [-10, 7, 3]), arrayMaxIndex(x -> abs(x), [-10, 7, 3]);
SELECT arrayMinIndex(x -> 1, [10, 20, 30]), arrayMaxIndex(x -> 1, [10, 20, 30]);
SELECT arrayMinIndex(x, y -> x + y, [3, 1, 1, 5], [1, 4, 4, 0]), arrayMaxIndex(x, y -> x + y, [3, 1, 1, 5], [1, 4, 4, 0]);

-- Nullable and floating-point values.
SELECT arrayMinIndex([NULL, 2, 1]), arrayMaxIndex([NULL, 2, 1]);
SELECT arrayMinIndex([NULL::Nullable(Int64), NULL::Nullable(Int64)]), arrayMaxIndex([NULL::Nullable(Int64), NULL::Nullable(Int64)]);
SELECT arrayMinIndex([nan, 2.0, 1.0]), arrayMaxIndex([nan, 2.0, 1.0]);
SELECT arrayMinIndex([nan, nan]), arrayMaxIndex([nan, nan]);
SELECT arrayMinIndex([0.0, -0.0]), arrayMaxIndex([0.0, -0.0]);
SELECT arrayMinIndex([-inf, 0.0, -inf]), arrayMaxIndex([inf, 0.0, inf]);

-- Other comparable types.
SELECT arrayMinIndex(['b', 'a', 'a']), arrayMaxIndex(['b', 'a', 'a']);
SELECT arrayMinIndex([(2, 'b'), (1, 'a'), (1, 'a')]), arrayMaxIndex([(2, 'b'), (1, 'a'), (1, 'a')]);
SELECT arrayMinIndex([toDecimal32(2, 2), toDecimal32(1, 2), toDecimal32(1, 2)]), arrayMaxIndex([toDecimal32(2, 2), toDecimal32(1, 2), toDecimal32(1, 2)]);
SELECT arrayMinIndex([toDate('2024-01-02'), toDate('2024-01-01'), toDate('2024-01-01')]), arrayMaxIndex([toDate('2024-01-02'), toDate('2024-01-01'), toDate('2024-01-01')]);
SELECT arrayMinIndex([toDateTime64('2024-01-03 00:00:00', 3), toDateTime64('2024-01-01 00:00:00', 3), toDateTime64('2024-01-01 00:00:00', 3)]), arrayMaxIndex([toDateTime64('2024-01-03 00:00:00', 3), toDateTime64('2024-01-01 00:00:00', 3), toDateTime64('2024-01-01 00:00:00', 3)]);
SELECT arrayMinIndex([toInt128(0), toInt128(-1), toInt128(-1), toInt128(1)]), arrayMaxIndex([toUInt128(0), toUInt128(2), toUInt128(2), toUInt128(1)]);
SELECT arrayMinIndex([toInt256(0), toInt256(-1), toInt256(-1), toInt256(1)]), arrayMaxIndex([toUInt256(0), toUInt256(2), toUInt256(2), toUInt256(1)]);
SELECT arrayMinIndex([toDecimal128(2, 2), toDecimal128(1, 2), toDecimal128(1, 2)]), arrayMaxIndex([toDecimal256(2, 2), toDecimal256(3, 2), toDecimal256(3, 2)]);
