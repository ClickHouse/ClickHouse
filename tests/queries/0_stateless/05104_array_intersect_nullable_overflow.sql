-- The guard that drops elements which do not survive the cast to the common element type must fire
-- for `Array(Nullable(T))` arguments too, so that both spellings agree.

SELECT arrayIntersect([1], [257]);
SELECT arrayIntersect([toNullable(1)], [toNullable(257)]);
SELECT arrayIntersect([toNullable(257)], [toNullable(1)]);
SELECT arraySort(arrayIntersect([toNullable(1), toNullable(2)], [toNullable(257), toNullable(2)]));
SELECT arraySort(arrayIntersect([toLowCardinality(toNullable(1))], [toLowCardinality(toNullable(257))]));
SELECT arraySort(arrayUnion([1], [toNullable(257)]));
