-- `Boost.Geometry` overlay used to produce empty or non-finite results for these valid triangles.
-- Check the repaired result against areas obtained by exact rational clipping of the scaled inputs.
-- The scaled union area is 46134368109823041 / 2908670000000000.
-- The scaled intersection area is 372640893994186144770130259 / 1927935125323876800000000000.

SELECT 'union_extreme_overlay_result';
SELECT length(result), round(polygonAreaCartesian(CAST(arrayMap(polygon -> arrayMap(ring ->
    arrayMap(point -> (point.1 / 1e117, point.2 / 1e117), ring), polygon), result), 'MultiPolygon')), 8)
FROM (SELECT groupPolygonUnion(
    if(
        number = 16,
        readWKTPolygon('POLYGON ((5.82224e117 2.58796e117, 5.82224e117 8.62993e117, 8.73091e117 2.58796e117, 5.82224e117 2.58796e117))'),
        readWKTPolygon('POLYGON ((8.33338e117 1.15146e117, 8.33338e117 5.33529e117, 1.17934e118 1.15146e117, 8.33338e117 1.15146e117))')))
AS result FROM numbers(17));

SELECT 'intersection_extreme_overlay_result';
SELECT length(result), round(polygonAreaCartesian(CAST(arrayMap(polygon -> arrayMap(ring ->
    arrayMap(point -> (point.1 / 1e103, point.2 / 1e103), ring), polygon), result), 'MultiPolygon')), 8)
FROM (SELECT groupPolygonIntersection(
    if(
        number = 8,
        readWKTPolygon('POLYGON ((-1.12235e102 -3.64494e103, -1.0247e102 -2.95622e103, 2.32442e103 -3.63518e103, -1.12235e102 -3.64494e103))'),
        readWKTPolygon('POLYGON ((-1.33899e103 -3.63167e103, -1.33899e103 -2.36436e103, 6.75291e102 -3.63167e103, -1.33899e103 -3.63167e103))')))
AS result FROM numbers(9));
