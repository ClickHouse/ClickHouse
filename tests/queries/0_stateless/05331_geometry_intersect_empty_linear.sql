-- An empty linear geometry intersects nothing.

SELECT 'Cartesian, empty LineString vs areal';
SELECT geometryIntersectCartesian(CAST('[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]', 'Polygon'), CAST('[]', 'LineString'));
SELECT geometryIntersectCartesian(CAST('[]', 'LineString'), CAST('[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]', 'Polygon'));
SELECT geometryIntersectCartesian(CAST('[]', 'LineString'), CAST('[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]', 'Ring'));
SELECT geometryIntersectCartesian(CAST('[]', 'LineString'), CAST('[[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]]', 'MultiPolygon'));

SELECT 'Cartesian, MultiLineString with an empty element vs areal';
SELECT geometryIntersectCartesian(CAST('[[]]', 'MultiLineString'), CAST('[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]', 'Polygon'));
SELECT geometryIntersectCartesian(CAST('[[], [(5., 5.), (6., 6.)]]', 'MultiLineString'), CAST('[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]', 'Polygon'));
SELECT geometryIntersectCartesian(CAST('[[(5., 5.), (6., 6.)], []]', 'MultiLineString'), CAST('[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]', 'Polygon'));
SELECT geometryIntersectCartesian(CAST('[[], [(0.5, 0.5), (1.5, 1.5)]]', 'MultiLineString'), CAST('[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]', 'Polygon'));

SELECT 'Cartesian, non-constant LineString';
SELECT geometryIntersectCartesian(CAST('[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]', 'Polygon'), CAST(arrayJoin(['[(0.5, 0.5), (1.5, 1.5)]', '[]', '[(5., 5.), (6., 6.)]', '[]', '[(-1., 1.), (3., 1.)]']), 'LineString'));

SELECT 'Cartesian, Geometry';
SELECT geometryIntersectCartesian(readWKT('LINESTRING EMPTY'), readWKT('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'));

SELECT 'Spherical';
SELECT geometryIntersectSpherical(CAST('[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]', 'Polygon'), CAST('[]', 'LineString'));
SELECT geometryIntersectSpherical(CAST('[[], [(5., 5.), (6., 6.)]]', 'MultiLineString'), CAST('[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]', 'Polygon'));

SELECT 'polygonsDistance, MultiLineString with an empty element';
SELECT polygonsDistanceCartesian(CAST('[[], [(10., 10.), (11., 11.)]]', 'MultiLineString'), CAST('[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]', 'Polygon'));
SELECT polygonsDistanceCartesian(CAST('[[(10., 10.), (11., 11.)]]', 'MultiLineString'), CAST('[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]', 'Polygon'));
SELECT polygonsDistanceCartesian(CAST('[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]', 'Polygon'), CAST('[[(10., 10.), (11., 11.)], []]', 'MultiLineString'));
SELECT polygonsDistanceCartesian(CAST('[[], [(1., 1.), (1.5, 1.5)]]', 'MultiLineString'), CAST('[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]', 'Polygon'));

SELECT 'polygonsDistanceSpherical, MultiLineString with an empty element';
SELECT polygonsDistanceSpherical(CAST('[[], [(10., 10.), (11., 11.)]]', 'MultiLineString'), CAST('[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]', 'Polygon')) = polygonsDistanceSpherical(CAST('[[(10., 10.), (11., 11.)]]', 'MultiLineString'), CAST('[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]', 'Polygon'));
SELECT polygonsDistanceSpherical(CAST('[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]', 'Polygon'), CAST('[[(10., 10.), (11., 11.)], []]', 'MultiLineString')) = polygonsDistanceSpherical(CAST('[[(10., 10.), (11., 11.)]]', 'MultiLineString'), CAST('[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]', 'Polygon'));
SELECT polygonsDistanceSpherical(CAST('[[(10., 10.), (11., 11.)]]', 'MultiLineString'), CAST('[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]', 'Polygon')) > 0;
