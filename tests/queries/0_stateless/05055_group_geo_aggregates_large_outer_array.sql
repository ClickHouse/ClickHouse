-- ColumnArray::get() rejects arrays with more than 1,000,000 elements. These
-- geometries stay well within the aggregate point budgets because their nested
-- values are empty, and must work through the direct-column path.

SELECT 'convex_hull_large_multilinestring';
SELECT length(groupConvexHull(value))
FROM
(
    SELECT arrayMap(_ -> []::LineString, range(1000001))::MultiLineString AS value
);

SELECT 'union_large_multipolygon';
SELECT wkt(groupPolygonUnion(value))
FROM
(
    SELECT arrayMap(_ -> []::Polygon, range(1000001))::MultiPolygon AS value
);

SELECT 'intersection_large_multipolygon';
SELECT wkt(groupPolygonIntersection(value))
FROM
(
    SELECT arrayMap(_ -> []::Polygon, range(1000001))::MultiPolygon AS value
);
