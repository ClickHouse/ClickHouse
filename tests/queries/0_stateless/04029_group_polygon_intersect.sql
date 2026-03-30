-- Test groupPolygonIntersection aggregate function

-- Two overlapping squares: intersection is the 1x1 overlap
SELECT 'two_overlapping';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(p)), 2) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'),
        readWKTPolygon('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')
    ]) AS p
);

-- Two disjoint squares: intersection is empty
SELECT 'disjoint';
SELECT wkt(groupPolygonIntersection(p)) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'),
        readWKTPolygon('POLYGON ((5 5, 5 6, 6 6, 6 5, 5 5))')
    ]) AS p
);

-- Single polygon: intersection = itself
SELECT 'single';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(p)), 2) FROM (
    SELECT readWKTPolygon('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))') AS p
);

-- Three overlapping: common overlap area
SELECT 'three_overlapping';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(p)), 2) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))'),
        readWKTPolygon('POLYGON ((1 0, 1 3, 4 3, 4 0, 1 0))'),
        readWKTPolygon('POLYGON ((2 0, 2 3, 5 3, 5 0, 2 0))')
    ]) AS p
);

-- Containment: small inside big -> intersection = small
SELECT 'containment';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(p)), 2) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'),
        readWKTPolygon('POLYGON ((2 2, 2 4, 4 4, 4 2, 2 2))')
    ]) AS p
);

-- Ring input
SELECT 'ring_input';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(r)), 2) FROM (
    SELECT arrayJoin([
        readWKTRing('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'),
        readWKTRing('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')
    ]) AS r
);

-- MultiPolygon input
SELECT 'multipolygon_input';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(mp)), 2) FROM (
    SELECT arrayJoin([
        readWKTMultiPolygon('MULTIPOLYGON (((0 0, 0 3, 3 3, 3 0, 0 0)))'),
        readWKTMultiPolygon('MULTIPOLYGON (((1 1, 1 4, 4 4, 4 1, 1 1)))')
    ]) AS mp
);

-- Empty group
SELECT 'empty_group';
SELECT wkt(groupPolygonIntersection(p)) FROM (
    SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p WHERE 0
);

-- GROUP BY
SELECT 'group_by';
SELECT g, round(polygonAreaCartesian(groupPolygonIntersection(p)), 2) FROM (
    SELECT 1 AS g, readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))') AS p
    UNION ALL SELECT 1, readWKTPolygon('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')
    UNION ALL SELECT 2, readWKTPolygon('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))')
    UNION ALL SELECT 2, readWKTPolygon('POLYGON ((5 5, 5 15, 15 15, 15 5, 5 5))')
) GROUP BY g ORDER BY g;

-- Short-circuit: after disjoint pair, third polygon should not matter
SELECT 'short_circuit';
SELECT wkt(groupPolygonIntersection(p)) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'),
        readWKTPolygon('POLYGON ((10 10, 10 11, 11 11, 11 10, 10 10))'),
        readWKTPolygon('POLYGON ((0 0, 0 100, 100 100, 100 0, 0 0))')
    ]) AS p
);

-- Verify geometric equivalence
SELECT 'geometric_equivalence';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(
    groupPolygonIntersection(p),
    readWKTMultiPolygon('MULTIPOLYGON (((1 1, 1 2, 2 2, 2 1, 1 1)))')
)), 4) < 0.001
FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'),
        readWKTPolygon('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')
    ]) AS p
);
