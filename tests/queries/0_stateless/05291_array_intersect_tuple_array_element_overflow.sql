-- An element of type `Tuple` or `Array` is not in the intersection when the cast to the common element type
-- changes a value in it, as for a scalar element: the changed value can coincide with an element that is really there.

SELECT arrayIntersect([(300, 1), (2, 2)], [(44, 1), (2, 2)]);
SELECT arrayIntersect([(44, 1), (2, 2)], [(300, 1), (2, 2)]);
SELECT arrayIntersect([(-1, 'a'), (2, 'b')], [(toUInt8(255), 'a'), (toUInt8(2), 'b')]);
SELECT arrayIntersect([[300], [2]], [[44], [2]]);
SELECT arrayIntersect([[300, 2], [2]], [[44, 2], [2]]);
SELECT arrayIntersect([[]::Array(UInt16), [300]], [[]::Array(UInt8), [44]]);
SELECT arrayIntersect([((300, 1), 'a'), ((2, 2), 'b')], [((44, 1), 'a'), ((2, 2), 'b')]);
SELECT arrayIntersect([[(300, 1)], [(2, 2)]], [[(44, 1)], [(2, 2)]]);
SELECT arrayIntersect([(toDateTime('2025-01-01 12:00:00', 'UTC'), 1), (toDateTime('2025-01-02 00:00:00', 'UTC'), 2)], [(toDate('2025-01-01'), 1), (toDate('2025-01-02'), 2)]);

SELECT arrayIntersect(CAST([(300, 1), (5, 2)] AS Array(Tuple(UInt16, UInt8))), CAST([(44, 1), (5, 2)] AS Array(Tuple(Nullable(UInt8), UInt8))));
SELECT arrayIntersect(CAST([(300, 1), (2, 2)] AS Array(Nullable(Tuple(UInt16, UInt8)))), CAST([(44, 1), (2, 2)] AS Array(Nullable(Tuple(UInt8, UInt8)))));
SELECT arrayIntersect([[toNullable(toUInt16(300))], [7]], CAST([[44], [7]] AS Array(Array(Nullable(UInt8)))));
-- a NULL is a member whatever value is stored under it (here 300, which does not fit)
SELECT arrayIntersect([tuple(nullIf(materialize(toUInt16(300)), 300), 1), (7, 2)], CAST([(NULL, 1), (7, 2)] AS Array(Tuple(Nullable(UInt8), UInt8))));
SELECT arrayIntersect([[nullIf(materialize(toUInt16(300)), 300)], [7]], CAST([[NULL], [7]] AS Array(Array(Nullable(UInt8)))));

-- the result keeps the order of the first array, which is longer, then shorter than the second one
SELECT arrayIntersect(CAST([(257, 0), (2, 0), (1, 0)] AS Array(Tuple(UInt16, UInt8))), CAST([(1, 0), (2, 0)] AS Array(Tuple(UInt8, UInt8))));
SELECT arrayIntersect(CAST([(257, 0), (3, 0), (1, 0)] AS Array(Tuple(UInt16, UInt8))), CAST([(1, 0), (2, 0), (3, 0), (4, 0), (5, 0)] AS Array(Tuple(UInt8, UInt8))));

SELECT arrayIntersect(a, b) FROM values('a Array(Tuple(UInt16, UInt8)), b Array(Tuple(UInt8, UInt8))', ([(300, 1), (2, 2)], [(44, 1), (2, 2)]), ([(256, 0)], [(0, 0)]), ([(5, 5)], [(5, 5)]));
SELECT arrayIntersect([(300, 1), (2, 2)], [(44, 1), (2, 2)], [(44, 1), (2, 2), (3, 3)]);
SELECT arrayIntersect([(map(1, 2), 300), (map(1, 2), 2)], [(map(1, 2), toUInt8(44)), (map(1, 2), toUInt8(2))]);
SELECT arrayIntersect([(toLowCardinality('a'), 300), (toLowCardinality('b'), 2)], [('a', toUInt8(44)), ('b', toUInt8(2))]);
SELECT round(arrayJaccardIndex([(300, 1), (2, 2)], [(44, 1), (2, 2)]), 2);

-- unchanged: a cast to a narrower floating-point type rounds, and the rounded value is a member
SELECT arrayIntersect([(0.2, 1)], CAST([(0.2, 1)] AS Array(Tuple(Float32, UInt8))));
-- unchanged: union and symmetric difference do not narrow
SELECT arraySort(arrayUnion([(300, 1)], [(toUInt8(44), 1)])), arraySort(arraySymmetricDifference([(300, 1), (2, 2)], [(toUInt8(44), 1), (toUInt8(2), 2)]));
SELECT arraySort(arrayUnion([(1, NULL)], [('a'::Dynamic, 1)])), arraySort(arraySymmetricDifference([(toNullable(1), 2)], [('a'::Dynamic, 2)]));
SELECT n, arraySort(arrayUnion(n, [('a'::Dynamic, 1)])) FROM values('n Array(Tuple(Nullable(UInt8), UInt8))', [], [(1, 1), (NULL, 1)], [(NULL, 1)]) ORDER BY n;
