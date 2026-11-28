-- 8-bit types use a bitmap state; the elements are emitted in ascending unsigned order.

SELECT groupUniqArray(toUInt8(number % 7 * 40)) FROM numbers(100);
SELECT groupUniqArray(toUInt8(number)) FROM numbers(0);
SELECT length(groupUniqArray(toUInt8(number))) FROM numbers(100000);

SELECT groupUniqArray(toInt8(arrayJoin([-128, -1, 0, 1, 127, -128, 0])));

SELECT groupUniqArray(x), toTypeName(groupUniqArray(x)) FROM (SELECT CAST(arrayJoin(['b', 'a', 'b', 'c']), 'Enum8(\'a\' = -1, \'b\' = 1, \'c\' = 2)') AS x);

SELECT groupUniqArray(CAST(number % 2, 'Bool')), toTypeName(groupUniqArray(CAST(number % 2, 'Bool'))) FROM numbers(10);

SELECT groupUniqArray(nullIf(toUInt8(number % 3), 1)) FROM numbers(10);

-- max_size: elements that are already present do not count against the limit.
SELECT groupUniqArray(3)(toUInt8(arrayJoin([5, 5, 5, 4, 4, 3, 2, 1, 5, 4, 3])));
SELECT length(groupUniqArray(300)(toUInt8(number))) FROM numbers(1000);

SELECT groupUniqArrayIf(toUInt8(number % 10), number % 2 = 1) FROM numbers(100);
SELECT k, groupUniqArrayIf(toUInt8(number % 5), number % 2 = 0) FROM numbers(50) GROUP BY number % 3 AS k ORDER BY k;

SELECT groupUniqArrayMerge(s) FROM (SELECT groupUniqArrayState(toUInt8(number % 20)) AS s FROM numbers(1000) GROUP BY number % 7);
SELECT length(groupUniqArrayMerge(4)(s)) FROM (SELECT groupUniqArrayState(4)(toUInt8(number % 20)) AS s FROM numbers(1000) GROUP BY number % 7);

-- Limited merge takes the elements of the right-hand side in ascending order until the limit is reached.
SELECT groupUniqArrayMerge(2)(CAST(unhex('03010203'), 'AggregateFunction(groupUniqArray(2), UInt8)'));
SELECT arrayReduce('groupUniqArrayMerge(3)', [CAST(unhex('020507'), 'AggregateFunction(groupUniqArray(3), UInt8)'), CAST(unhex('03010203'), 'AggregateFunction(groupUniqArray(3), UInt8)')]);

-- Serialized state: size, then one byte per element.
SELECT hex(groupUniqArrayState(toUInt8(number % 5 * 3))) FROM numbers(100);
SELECT hex(groupUniqArrayState(toInt8(arrayJoin([-1, 1, -128]))));
SELECT hex(groupUniqArrayState(toUInt8(number))) FROM numbers(0);

-- States written by the hash set implementation (elements in arbitrary order) are readable.
SELECT finalizeAggregation(CAST(unhex('0403010204'), 'AggregateFunction(groupUniqArray, UInt8)'));
SELECT finalizeAggregation(CAST(unhex('02FF01'), 'AggregateFunction(groupUniqArray, Int8)'));
SELECT finalizeAggregation(CAST(unhex('0201FF'), 'AggregateFunction(groupUniqArray, Enum8(\'a\' = -1, \'b\' = 1))'));

SELECT finalizeAggregation(CAST(unhex('8102' || repeat('00', 257)), 'AggregateFunction(groupUniqArray, UInt8)')); -- { serverError TOO_LARGE_ARRAY_SIZE }
