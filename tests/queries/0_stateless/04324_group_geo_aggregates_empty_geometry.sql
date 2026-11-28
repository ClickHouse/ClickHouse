-- Test empty geometry semantics for union and intersect

-- 1. Union: empty polygon is neutral element
SELECT 'union_empty_neutral';
SELECT round(polygonAreaCartesian(groupPolygonUnion(p)), 2) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON EMPTY'),
        readWKTPolygon('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))')
    ]) AS p
);

-- 2. Intersect: empty polygon is absorbing element
SELECT 'intersect_empty_absorbing';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(p)), 4) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON EMPTY'),
        readWKTPolygon('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))')
    ]) AS p
);

-- 3. Union of only empty polygons -> empty MultiPolygon
SELECT 'union_all_empty';
SELECT wkt(groupPolygonUnion(p)) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON EMPTY'),
        readWKTPolygon('POLYGON EMPTY')
    ]) AS p
);

-- 4. Intersect of only empty polygons -> empty MultiPolygon
SELECT 'intersect_all_empty';
SELECT wkt(groupPolygonIntersection(p)) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON EMPTY'),
        readWKTPolygon('POLYGON EMPTY')
    ]) AS p
);

-- 5. Union: empty Geometry variant is also neutral
SELECT 'union_empty_geometry';
SELECT round(polygonAreaCartesian(groupPolygonUnion(g)), 2) FROM (
    SELECT arrayJoin([
        readWKT('POLYGON EMPTY'),
        readWKT('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))')
    ]) AS g
);

-- 6. Intersect: empty Geometry variant is also absorbing
SELECT 'intersect_empty_geometry';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(g)), 4) FROM (
    SELECT arrayJoin([
        readWKT('POLYGON EMPTY'),
        readWKT('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))')
    ]) AS g
);

-- 7. Union: geometric equivalence check (empty + polygon = polygon)
SELECT 'union_empty_geometric_equiv';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(
    actual,
    [[[(0., 0.), (0., 3.), (3., 3.), (3., 0.), (0., 0.)]]]::MultiPolygon
)), 4) < 0.001 AS equiv
FROM (
    SELECT groupPolygonUnion(p) AS actual FROM (
        SELECT arrayJoin([
            readWKTPolygon('POLYGON EMPTY'),
            readWKTPolygon('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))')
        ]) AS p
    )
);

-- 8. Empty polygon through State/Merge path
SELECT 'union_empty_state_merge';
SELECT round(polygonAreaCartesian(groupPolygonUnionMerge(state)), 2) FROM (
    SELECT groupPolygonUnionState(p) AS state FROM (
        SELECT readWKTPolygon('POLYGON EMPTY') AS p
    )
    UNION ALL
    SELECT groupPolygonUnionState(p) AS state FROM (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))') AS p
    )
);
