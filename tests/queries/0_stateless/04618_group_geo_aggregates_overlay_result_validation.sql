-- Boost.Geometry overlay can fail numerically on finite, individually valid inputs with extreme
-- coordinates. A reduction result must satisfy the same finite/topology invariants as a serialized
-- chunk; otherwise the aggregate writer emits bytes that its own reader rejects.

SELECT 'union_rejects_empty_overlay_result';
SELECT groupPolygonUnion(
    if(
        number = 16,
        readWKTPolygon('POLYGON ((5.82224e117 2.58796e117, 5.82224e117 8.62993e117, 8.73091e117 2.58796e117, 5.82224e117 2.58796e117))'),
        readWKTPolygon('POLYGON ((8.33338e117 1.15146e117, 8.33338e117 5.33529e117, 1.17934e118 1.15146e117, 8.33338e117 1.15146e117))')))
FROM numbers(17); -- { serverError BAD_ARGUMENTS }

SELECT 'intersection_rejects_non_finite_overlay_result';
SELECT groupPolygonIntersection(
    if(
        number = 8,
        readWKTPolygon('POLYGON ((-1.12235e102 -3.64494e103, -1.0247e102 -2.95622e103, 2.32442e103 -3.63518e103, -1.12235e102 -3.64494e103))'),
        readWKTPolygon('POLYGON ((-1.33899e103 -3.63167e103, -1.33899e103 -2.36436e103, 6.75291e102 -3.63167e103, -1.33899e103 -3.63167e103))')))
FROM numbers(9); -- { serverError BAD_ARGUMENTS }
