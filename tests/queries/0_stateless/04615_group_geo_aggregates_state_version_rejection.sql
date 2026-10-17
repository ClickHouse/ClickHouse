-- All geo aggregate state readers must reject an unknown binary format version. The convex-hull
-- path has its own format and is covered in `04328`; these cases cover both polygonal readers.

SELECT 'union_bad_version';
SELECT groupPolygonUnionMerge(state)
FROM
(
    SELECT CAST(unhex(concat(
        'FF',
        substring(hex(groupPolygonUnionState(p)), 3)
    )) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state
    FROM
    (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p
    )
); -- { serverError INCORRECT_DATA }

SELECT 'intersect_bad_version';
SELECT groupPolygonIntersectionMerge(state)
FROM
(
    SELECT CAST(unhex(concat(
        'FF',
        substring(hex(groupPolygonIntersectionState(p)), 3)
    )) AS AggregateFunction(groupPolygonIntersection, Polygon)) AS state
    FROM
    (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p
    )
); -- { serverError INCORRECT_DATA }
