-- Test 1: Single polygon (identity)
SELECT groupPolygonUnion([[[(0., 0.), (0., 5.), (5., 5.), (5., 0.)]]]);

-- Test 2: Two overlapping polygons (should merge into one - deterministic)
SELECT wkt(groupPolygonUnion(polygon)) FROM (
    SELECT [[[(0., 0.), (0., 5.), (5., 5.), (5., 0.), (0., 0.)]]] AS polygon
    UNION ALL
    SELECT [[[(3., 3.), (3., 8.), (8., 8.), (8., 3.), (3., 3.)]]] AS polygon
);

-- Test 3: Check result is MultiPolygon type
SELECT toTypeName(groupPolygonUnion([[[(0., 0.), (0., 5.), (5., 5.), (5., 0.)]]]));

-- Test 4: Empty check with GROUP BY
SELECT id, notEmpty(groupPolygonUnion(polygon)) AS has_result FROM (
    SELECT 1 AS id, [[[(0., 0.), (0., 2.), (2., 2.), (2., 0.), (0., 0.)]]] AS polygon
    UNION ALL
    SELECT 1 AS id, [[[(1., 1.), (1., 3.), (3., 3.), (3., 1.), (1., 1.)]]] AS polygon
    UNION ALL
    SELECT 2 AS id, [[[(10., 10.), (10., 12.), (12., 12.), (12., 10.), (10., 10.)]]] AS polygon
) GROUP BY id ORDER BY id;