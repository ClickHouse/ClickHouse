SELECT arrayMinIndex([5, 3, 2, 7]), arrayMaxIndex([5, 3, 2, 7]);
SELECT arrayMinIndex([5, 3, 3, 7]), arrayMaxIndex([5, 7, 7, 3]);
SELECT arrayMinIndex(emptyArrayInt32()), arrayMaxIndex(emptyArrayUInt64());
SELECT arrayMinIndex([42]), arrayMaxIndex([42]);
SELECT arrayMinIndex(['b', 'a', 'a']), arrayMaxIndex(['b', 'a', 'a']);
SELECT arrayMinIndex(x -> abs(x), [-10, 7, 3]), arrayMaxIndex(x -> abs(x), [-10, 7, 3]);
SELECT arrayMinIndex(x -> 1, [1, 2, 3]), arrayMaxIndex(x -> 1, [1, 2, 3]);
SELECT arrayMinIndex(x, y -> x * y, [1, 5, 3], [2, 2, 5]), arrayMaxIndex(x, y -> x * y, [1, 5, 3], [2, 2, 5]);
SELECT arrayMinIndex([nan, 2.0, 1.0]), arrayMaxIndex([nan, 2.0, 1.0]);
SELECT arrayMinIndex([nan, nan]), arrayMaxIndex([nan, nan]);
SELECT arrayMinIndex([0.0, -0.0]), arrayMaxIndex([0.0, -0.0]);
SELECT arrayMinIndex(range(128)), arrayMaxIndex(range(128));
SELECT arrayMinIndex(range(16384)), arrayMaxIndex(range(16384));
SELECT arrayMinIndex(a), arrayMaxIndex(a)
FROM (SELECT arrayJoin([range(128), arrayReverse(range(128)), range(64)]) AS a)
ORDER BY length(a), arrayMax(a);
