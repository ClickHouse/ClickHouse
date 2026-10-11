-- Test groupPolygonUnion aggregate function

-- Two overlapping squares: their union should cover the L-shaped area
SELECT 'two_overlapping';
SELECT round(polygonAreaCartesian(groupPolygonUnion(p)), 2) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'),
        readWKTPolygon('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')
    ]) AS p
);

-- Two disjoint squares
SELECT 'two_disjoint';
SELECT round(polygonAreaCartesian(groupPolygonUnion(p)), 2) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'),
        readWKTPolygon('POLYGON ((5 5, 5 6, 6 6, 6 5, 5 5))')
    ]) AS p
);

-- Single polygon
SELECT 'single';
SELECT round(polygonAreaCartesian(groupPolygonUnion(p)), 2) FROM (
    SELECT readWKTPolygon('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))') AS p
);

-- Three overlapping squares
SELECT 'three_overlapping';
SELECT round(polygonAreaCartesian(groupPolygonUnion(p)), 2) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'),
        readWKTPolygon('POLYGON ((1 0, 1 2, 3 2, 3 0, 1 0))'),
        readWKTPolygon('POLYGON ((2 0, 2 2, 4 2, 4 0, 2 0))')
    ]) AS p
);

-- Ring input
SELECT 'ring_input';
SELECT round(polygonAreaCartesian(groupPolygonUnion(r)), 2) FROM (
    SELECT arrayJoin([
        readWKTRing('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'),
        readWKTRing('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')
    ]) AS r
);

-- MultiPolygon input
SELECT 'multipolygon_input';
SELECT round(polygonAreaCartesian(groupPolygonUnion(mp)), 2) FROM (
    SELECT arrayJoin([
        readWKTMultiPolygon('MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0, 0 0)))'),
        readWKTMultiPolygon('MULTIPOLYGON (((0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5)))')
    ]) AS mp
);

-- Empty group
SELECT 'empty_group';
SELECT wkt(groupPolygonUnion(p)) FROM (
    SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p WHERE 0
);

-- GROUP BY
SELECT 'group_by';
SELECT g, round(polygonAreaCartesian(groupPolygonUnion(p)), 2) FROM (
    SELECT 1 AS g, readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))') AS p
    UNION ALL SELECT 1, readWKTPolygon('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')
    UNION ALL SELECT 2, readWKTPolygon('POLYGON ((10 10, 10 20, 20 20, 20 10, 10 10))')
    UNION ALL SELECT 2, readWKTPolygon('POLYGON ((15 15, 15 25, 25 25, 25 15, 15 15))')
) GROUP BY g ORDER BY g;

-- Containment: small square inside big square -> area = big square
SELECT 'containment';
SELECT round(polygonAreaCartesian(groupPolygonUnion(p)), 2) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'),
        readWKTPolygon('POLYGON ((2 2, 2 4, 4 4, 4 2, 2 2))')
    ]) AS p
);

-- Verify geometric equivalence of union: area(symDiff(actual, expected)) should be ~0
SELECT 'geometric_equivalence';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(
    groupPolygonUnion(p),
    readWKTMultiPolygon('MULTIPOLYGON (((0 0, 0 2, 1 2, 1 3, 3 3, 3 1, 2 1, 2 0, 0 0)))')
)), 4) < 0.001
FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'),
        readWKTPolygon('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')
    ]) AS p
);
