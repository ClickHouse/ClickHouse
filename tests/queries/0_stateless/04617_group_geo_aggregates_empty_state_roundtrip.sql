-- Writer-produced empty states are part of the binary format. Check both byte-for-byte
-- reserialization and Merge results so future reader hardening cannot accidentally reject them.

SELECT 'union_empty_state';
SELECT
    serialized = '0100' AS writer_bytes,
    hex(CAST(unhex(serialized) AS AggregateFunction(groupPolygonUnion, Polygon))) = serialized AS bytes_equal,
    empty(groupPolygonUnionMerge(CAST(unhex(serialized) AS AggregateFunction(groupPolygonUnion, Polygon)))) AS result_empty
FROM
(
    SELECT hex(groupPolygonUnionState(p)) AS serialized
    FROM (SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p WHERE false)
);

SELECT 'intersect_uninitialized_state';
SELECT
    serialized = '0100' AS writer_bytes,
    hex(CAST(unhex(serialized) AS AggregateFunction(groupPolygonIntersection, Polygon))) = serialized AS bytes_equal,
    empty(groupPolygonIntersectionMerge(CAST(unhex(serialized) AS AggregateFunction(groupPolygonIntersection, Polygon)))) AS result_empty
FROM
(
    SELECT hex(groupPolygonIntersectionState(p)) AS serialized
    FROM (SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p WHERE false)
);

SELECT 'intersect_empty_state';
SELECT
    serialized = '0102' AS writer_bytes,
    hex(CAST(unhex(serialized) AS AggregateFunction(groupPolygonIntersection, Polygon))) = serialized AS bytes_equal,
    empty(groupPolygonIntersectionMerge(CAST(unhex(serialized) AS AggregateFunction(groupPolygonIntersection, Polygon)))) AS result_empty
FROM
(
    SELECT hex(groupPolygonIntersectionState([]::Polygon)) AS serialized
);

SELECT 'convex_hull_empty_state';
SELECT
    serialized = '020000' AS writer_bytes,
    hex(CAST(unhex(serialized) AS AggregateFunction(groupConvexHull, Point))) = serialized AS bytes_equal,
    empty(groupConvexHullMerge(CAST(unhex(serialized) AS AggregateFunction(groupConvexHull, Point)))) AS result_empty
FROM
(
    SELECT hex(groupConvexHullState(p)) AS serialized
    FROM (SELECT readWKTPoint('POINT (0 0)') AS p WHERE false)
);
