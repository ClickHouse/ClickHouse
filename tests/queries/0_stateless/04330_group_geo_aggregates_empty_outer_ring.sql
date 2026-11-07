-- A polygon with an empty outer ring but non-empty inner rings is malformed.
-- It must be rejected instead of silently treated as an empty geometry, otherwise
-- its inner rings (which may carry invalid topology or out-of-range coordinates)
-- would bypass validation.

-- Polygon path: empty outer, one non-empty inner ring.
SELECT 'union_polygon_empty_outer_nonempty_inner';
SELECT groupPolygonUnion(p) FROM (
    SELECT [[], [(0., 0.), (0., 1.), (1., 1.), (1., 0.), (0., 0.)]]::Polygon AS p
); -- { serverError BAD_ARGUMENTS }

SELECT 'intersect_polygon_empty_outer_nonempty_inner';
SELECT groupPolygonIntersection(p) FROM (
    SELECT [[], [(0., 0.), (0., 1.), (1., 1.), (1., 0.), (0., 0.)]]::Polygon AS p
); -- { serverError BAD_ARGUMENTS }

-- MultiPolygon path: one polygon with empty outer, one non-empty inner ring.
SELECT 'union_multipolygon_empty_outer_nonempty_inner';
SELECT groupPolygonUnion(p) FROM (
    SELECT [[[], [(0., 0.), (0., 1.), (1., 1.), (1., 0.), (0., 0.)]]]::MultiPolygon AS p
); -- { serverError BAD_ARGUMENTS }

-- A fully empty polygon (empty outer and no inner rings) is the neutral element and
-- is skipped: the union over only empty polygons is an empty MultiPolygon.
SELECT 'union_fully_empty_polygon_is_skipped';
SELECT groupPolygonUnion(p) FROM (SELECT []::Polygon AS p);
