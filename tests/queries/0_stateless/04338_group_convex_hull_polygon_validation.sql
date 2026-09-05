-- groupConvexHull builds the hull from outer-ring points only, but it still validates the whole
-- polygonal value: inner-ring (hole) coordinates must be finite, and a polygon with an empty outer
-- ring but non-empty inner rings is malformed. Such inputs must be rejected rather than silently
-- accepted just because only outer-ring points contribute to the hull.

-- Polygon: empty outer ring, one non-empty inner ring.
SELECT 'convexhull_polygon_empty_outer_nonempty_inner';
SELECT groupConvexHull(p) FROM (
    SELECT [[], [(0., 0.), (0., 1.), (1., 1.), (1., 0.), (0., 0.)]]::Polygon AS p
); -- { serverError BAD_ARGUMENTS }

-- MultiPolygon: one polygon with empty outer ring and one non-empty inner ring.
SELECT 'convexhull_multipolygon_empty_outer_nonempty_inner';
SELECT groupConvexHull(p) FROM (
    SELECT [[[], [(0., 0.), (0., 1.), (1., 1.), (1., 0.), (0., 0.)]]]::MultiPolygon AS p
); -- { serverError BAD_ARGUMENTS }

-- Polygon: valid outer ring, NaN coordinate inside a hole.
SELECT 'convexhull_polygon_nan_in_hole';
SELECT groupConvexHull(p) FROM (
    SELECT [[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(nan, 2.), (2., 3.), (3., 3.), (3., 2.), (nan, 2.)]]::Polygon AS p
); -- { serverError BAD_ARGUMENTS }

-- Polygon: valid outer ring, +Inf coordinate inside a hole.
SELECT 'convexhull_polygon_inf_in_hole';
SELECT groupConvexHull(p) FROM (
    SELECT [[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(2., inf), (2., 3.), (3., 3.), (3., 2.), (2., inf)]]::Polygon AS p
); -- { serverError BAD_ARGUMENTS }

-- MultiPolygon: valid outer ring, NaN coordinate inside a hole of one polygon.
SELECT 'convexhull_multipolygon_nan_in_hole';
SELECT groupConvexHull(p) FROM (
    SELECT [[[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(nan, 2.), (2., 3.), (3., 3.), (3., 2.), (nan, 2.)]]]::MultiPolygon AS p
); -- { serverError BAD_ARGUMENTS }

-- A fully empty polygon contributes no points (neutral) and must be accepted, not rejected.
-- All-empty input yields an empty hull; an empty polygon mixed with real geometries yields the
-- hull of the real ones only. The same holds when the value arrives through Geometry.

-- All-empty input, typed Polygon.
SELECT 'convexhull_empty_polygon_typed';
SELECT wkt(groupConvexHull(p)) FROM (SELECT readWKTPolygon('POLYGON EMPTY') AS p);

-- All-empty input, through Geometry.
SELECT 'convexhull_empty_polygon_geometry';
SELECT wkt(groupConvexHull(g)) FROM (SELECT readWKT('POLYGON EMPTY') AS g);

-- Empty polygon mixed with a real polygon, typed Polygon: hull comes from the real polygon only.
SELECT 'convexhull_empty_plus_polygon_typed';
SELECT wkt(groupConvexHull(p)) FROM (
    SELECT arrayJoin([readWKTPolygon('POLYGON EMPTY'), readWKTPolygon('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))')]) AS p
);

-- Empty polygon mixed with real points, through Geometry: hull comes from the points only.
SELECT 'convexhull_empty_plus_points_geometry';
SELECT wkt(groupConvexHull(g)) FROM (
    SELECT arrayJoin([readWKT('POLYGON EMPTY'), readWKT('POINT (0 0)'), readWKT('POINT (10 0)'), readWKT('POINT (10 10)'), readWKT('POINT (0 10)')]) AS g
);
