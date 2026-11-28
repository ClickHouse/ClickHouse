-- The guard that drops elements which do not survive the cast to the common element type must fire
-- for `Array(Nullable(T))` arguments too, so that both spellings agree.

SELECT arrayIntersect([1], [257]);
SELECT arrayIntersect([toNullable(1)], [toNullable(257)]);
SELECT arrayIntersect([toNullable(257)], [toNullable(1)]);
SELECT arraySort(arrayIntersect([toNullable(1), toNullable(2)], [toNullable(257), toNullable(2)]));
SELECT arraySort(arrayIntersect([toLowCardinality(toNullable(1))], [toLowCardinality(toNullable(257))]));
SELECT arraySort(arrayUnion([1], [toNullable(257)]));

-- A common element type that cannot be `Nullable` leaves the cast element column unwrapped while the
-- original one is not: the guard must not hand the comparison a column and a type that disagree.
SELECT arrayUnion([1, NULL], ['a'::Dynamic]);
SELECT arraySymmetricDifference([1, NULL], ['a'::Dynamic]);
SELECT arraySort(arraySymmetricDifference([toDate('2026-01-01'), NULL], ['a'::Dynamic]));
SELECT arraySort(arrayUnion([toDateTime('2026-01-01 12:00:00'), NULL], ['a'::Dynamic]));
SELECT arraySort(arrayUnion([toDateTime64('2026-01-01 12:00:00.123', 3), NULL], ['a'::Dynamic]));
SELECT arraySort(arrayUnion([toLowCardinality(toNullable(1))], ['a'::Dynamic]));
SELECT arraySort(arrayUnion(materialize([1, NULL]), materialize(['a'::Dynamic])));

-- A value present on both sides must still be deduplicated: the mask stays aligned with its column.
SELECT arraySort(arrayUnion([1, NULL], [CAST(1 AS UInt8)::Dynamic]));

-- An empty row has to ride along with a non-empty one: on its own the cast column has no variants,
-- so the comparison that carries the check is never dispatched.
SELECT n, arraySort(arrayUnion(n, ['a'::Dynamic])) FROM values('n Array(Nullable(UInt8))', [], [1, NULL]) ORDER BY n;
