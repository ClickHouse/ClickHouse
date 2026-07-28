-- Tests for geometryIntersectCartesian / geometryIntersectSpherical.
-- Unlike polygonsIntersect{Cartesian,Spherical}, these work for any geometry data type
-- (Point, LineString, MultiLineString, Ring, Polygon, MultiPolygon), including the Geometry
-- (Variant) type, and the two arguments may be of different types.

SELECT '-- Cartesian: Point vs areal';
SELECT geometryIntersectCartesian((1., 1.)::Point, [(0., 0.), (0., 2.), (2., 2.), (2., 0.)]::Ring);
SELECT geometryIntersectCartesian((5., 5.)::Point, [(0., 0.), (0., 2.), (2., 2.), (2., 0.)]::Ring);
SELECT geometryIntersectCartesian((1., 1.)::Point, [[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]::Polygon);
SELECT geometryIntersectCartesian((1., 1.)::Point, [[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]]::MultiPolygon);

SELECT '-- Cartesian: Point vs Point';
SELECT geometryIntersectCartesian((1., 1.)::Point, (1., 1.)::Point);
SELECT geometryIntersectCartesian((1., 1.)::Point, (2., 2.)::Point);

SELECT '-- Cartesian: linear vs areal / linear';
SELECT geometryIntersectCartesian([(-1., 1.), (3., 1.)]::LineString, [[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]::Polygon);
SELECT geometryIntersectCartesian([(5., 5.), (6., 6.)]::LineString, [[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]::Polygon);
SELECT geometryIntersectCartesian([(0., 0.), (2., 2.)]::LineString, [(0., 2.), (2., 0.)]::LineString);
SELECT geometryIntersectCartesian([(0., 0.), (2., 2.)]::LineString, [(3., 0.), (5., 2.)]::LineString);
SELECT geometryIntersectCartesian([[(-1., 1.), (3., 1.)]]::MultiLineString, [[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]::Polygon);
SELECT geometryIntersectCartesian([[(-1., 1.), (3., 1.)]]::MultiLineString, [(0., 0.), (2., 2.)]::LineString);

SELECT '-- Cartesian: areal vs areal';
SELECT geometryIntersectCartesian([(0., 0.), (0., 2.), (2., 2.), (2., 0.)]::Ring, [(1., 1.), (1., 3.), (3., 3.), (3., 1.)]::Ring);
SELECT geometryIntersectCartesian([(0., 0.), (0., 2.), (2., 2.), (2., 0.)]::Ring, [(5., 5.), (5., 7.), (7., 7.), (7., 5.)]::Ring);
SELECT geometryIntersectCartesian([[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]::Polygon, [[(5., 5.), (5., 7.), (7., 7.), (7., 5.)]]::Polygon);
SELECT geometryIntersectCartesian([[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]]::MultiPolygon, [[[(1., 1.), (1., 3.), (3., 3.), (3., 1.)]]]::MultiPolygon);

SELECT '-- Cartesian: Geometry (Variant) argument';
DROP TABLE IF EXISTS test_geo_04506;
CREATE TABLE test_geo_04506 (id UInt32, g Geometry) ENGINE = Memory;
INSERT INTO test_geo_04506 VALUES (1, readWKT('POINT(1 1)')), (2, readWKT('POINT(5 5)')), (3, readWKT('LINESTRING(-1 1, 3 1)')), (4, readWKT('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))')), (5, readWKT('MULTIPOLYGON(((5 5, 5 7, 7 7, 7 5, 5 5)))'));
SELECT id, geometryIntersectCartesian(g, [[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]::Polygon) FROM test_geo_04506 ORDER BY id;
-- Geometry vs Geometry (both Variant)
SELECT id, geometryIntersectCartesian(g, readWKT('POINT(1 1)')) FROM test_geo_04506 ORDER BY id;
DROP TABLE test_geo_04506;

SELECT '-- Spherical: basic';
SELECT geometryIntersectSpherical([[[(4.3613577, 50.8651821), (4.349556, 50.8535879), (4.3602419, 50.8435626), (4.3830299, 50.8428851), (4.3904543, 50.8564867), (4.3613148, 50.8651279)]]]::MultiPolygon, (4.36, 50.85)::Point);
SELECT geometryIntersectSpherical([[[(4.3613577, 50.8651821), (4.349556, 50.8535879), (4.3602419, 50.8435626), (4.3830299, 50.8428851), (4.3904543, 50.8564867), (4.3613148, 50.8651279)]]]::MultiPolygon, (10.0, 10.0)::Point);
SELECT geometryIntersectSpherical([(4.3613577, 50.8651821), (4.349556, 50.8535879), (4.3602419, 50.8435626), (4.3830299, 50.8428851)]::Ring, [(4.36, 50.85), (4.37, 50.86)]::LineString);

SELECT '-- Spherical: Geometry (Variant) argument';
SELECT geometryIntersectSpherical(readWKT('POLYGON((4.3613577 50.8651821, 4.349556 50.8535879, 4.3602419 50.8435626, 4.3830299 50.8428851, 4.3904543 50.8564867, 4.3613148 50.8651279, 4.3613577 50.8651821))'), (4.36, 50.85)::Point);

-- Ambiguous unnamed forms: LineString and Ring share the structural type Array(Tuple(Float64, Float64)),
-- and MultiLineString and Polygon share Array(Array(Tuple(Float64, Float64))). The explicitly-typed
-- linear and areal interpretations give different answers, so a value that lost its custom geo name
-- must not be silently reinterpreted - it is rejected instead.
SELECT '-- Ambiguity: linear vs areal give different results';
SELECT geometryIntersectCartesian([(0., 0.), (1., 0.), (1., 1.)]::LineString, (0.75, 0.25)::Point);
SELECT geometryIntersectCartesian([(0., 0.), (1., 0.), (1., 1.)]::Ring, (0.75, 0.25)::Point);
SELECT geometryIntersectCartesian([[(0., 0.), (1., 0.), (1., 1.)]]::MultiLineString, (0.75, 0.25)::Point);
SELECT geometryIntersectCartesian([[(0., 0.), (1., 0.), (1., 1.)]]::Polygon, (0.75, 0.25)::Point);

SELECT '-- Ambiguity: unnamed structural forms are rejected';
-- A plain literal, or an expression that strips the custom geo name, yields an unnamed Array(Tuple(Float64, Float64)).
SELECT geometryIntersectCartesian([(0., 0.), (1., 0.), (1., 1.)], (0.75, 0.25)::Point); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT geometryIntersectCartesian(arrayMap(p -> (p.1, p.2), [(0., 0.), (1., 0.), (1., 1.)]::LineString), (0.75, 0.25)::Point); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT geometryIntersectCartesian((0.75, 0.25)::Point, [[(0., 0.), (1., 0.), (1., 1.)]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT geometryIntersectSpherical([(0., 0.), (1., 0.), (1., 1.)], (0.75, 0.25)::Point); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- Unambiguous unnamed forms are still accepted';
-- Point (Tuple(Float64, Float64)) and MultiPolygon (Array(Array(Array(Tuple(Float64, Float64))))) have no ambiguous structural twin.
SELECT geometryIntersectCartesian((1., 1.), [[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]]);
SELECT geometryIntersectCartesian((5., 5.), [[[(0., 0.), (0., 2.), (2., 2.), (2., 0.)]]]);
