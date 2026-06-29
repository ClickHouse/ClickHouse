-- Exercise explicit binary serialize/deserialize round-trips for geo aggregate states.

-- 1. `groupConvexHull`: `hex` forces state serialization, `CAST(unhex(...))` forces deserialization.
SELECT 'convex_hull_binary_roundtrip';
SELECT round(polygonAreaCartesian(groupConvexHullMerge(state)), 2) FROM
(
    SELECT CAST(unhex(hex(groupConvexHullState(pt))) AS AggregateFunction(groupConvexHull, Point)) AS state
    FROM
    (
        SELECT arrayJoin([
            (0., 0.)::Point,
            (10., 0.)::Point,
            (10., 10.)::Point,
            (0., 10.)::Point
        ]) AS pt
    )
);

-- 2. `groupPolygonUnion`: polygon with an inner ring exercises ring/polygon state serialization helpers.
SELECT 'union_binary_roundtrip_hole';
SELECT round(polygonAreaCartesian(groupPolygonUnionMerge(state)), 2) FROM
(
    SELECT CAST(unhex(hex(groupPolygonUnionState(p))) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state
    FROM
    (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))') AS p
    )
);

-- 3. `groupPolygonIntersection`: same inner-ring round-trip through the explicit binary path.
SELECT 'intersect_binary_roundtrip_hole';
SELECT round(polygonAreaCartesian(groupPolygonIntersectionMerge(state)), 2) FROM
(
    SELECT CAST(unhex(hex(groupPolygonIntersectionState(p))) AS AggregateFunction(groupPolygonIntersection, Polygon)) AS state
    FROM
    (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))') AS p
    )
);

-- 4. `groupPolygonIntersection`: multi-row state round-trip exercises non-trivial chunk count serialization.
SELECT 'intersect_binary_roundtrip_two_chunks';
SELECT round(polygonAreaCartesian(groupPolygonIntersectionMerge(state)), 2) FROM
(
    SELECT CAST(unhex(hex(groupPolygonIntersectionState(p))) AS AggregateFunction(groupPolygonIntersection, Polygon)) AS state
    FROM
    (
        SELECT arrayJoin([
            readWKTPolygon('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))'),
            readWKTPolygon('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')
        ]) AS p
    )
);
