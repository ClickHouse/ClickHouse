-- Once intersection reaches its absorbing Empty state, later rows must not be
-- materialized, type-checked by active Geometry alternative, or validated.

SELECT 'explicit_empty_then_invalid_typed_polygon';
SELECT wkt(groupPolygonIntersection(p))
FROM
(
    SELECT arrayJoin([
        []::Polygon,
        [[(nan, 0.), (1., 0.), (1., 1.), (0., 1.), (nan, 0.)]]::Polygon
    ])::Polygon AS p
)
SETTINGS max_threads = 1;

SELECT 'reduced_to_empty_then_unsupported_geometry';
SELECT wkt(groupPolygonIntersection(g))
FROM
(
    SELECT arrayJoin([
        readWKT('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'),
        readWKT('POLYGON ((10 10, 10 11, 11 11, 11 10, 10 10))'),
        readWKT('POLYGON ((-1 -1, -1 12, 12 12, 12 -1, -1 -1))'),
        readWKT('POLYGON ((-2 -2, -2 13, 13 13, 13 -2, -2 -2))'),
        readWKT('POLYGON ((-3 -3, -3 14, 14 14, 14 -3, -3 -3))'),
        readWKT('POLYGON ((-4 -4, -4 15, 15 15, 15 -4, -4 -4))'),
        readWKT('POLYGON ((-5 -5, -5 16, 16 16, 16 -5, -5 -5))'),
        readWKT('POLYGON ((-6 -6, -6 17, 17 17, 17 -6, -6 -6))'),
        readWKT('POLYGON ((-7 -7, -7 18, 18 18, 18 -7, -7 -7))'),
        readWKT('POINT (1 1)')
    ]) AS g
)
SETTINGS max_threads = 1;
