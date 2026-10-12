-- { echoOn }
SELECT [(1, 2), (3, 4)]::MultiPoint AS mp, toTypeName(mp);
SELECT CAST([(1.5, 2.5), (3, 4)], 'MultiPoint') AS mp, toTypeName(mp);
SELECT []::MultiPoint AS mp, toTypeName(mp);

-- Casts between geo types with the same underlying structure.
SELECT [(1, 2), (3, 4)]::MultiPoint::Ring AS r, toTypeName(r);
SELECT [(1, 2), (3, 4)]::Ring::MultiPoint AS mp, toTypeName(mp);

-- MultiPoint column in a table.
DROP TABLE IF EXISTS geo_multipoint;
CREATE TABLE geo_multipoint (id Int32, mp MultiPoint) ENGINE = MergeTree ORDER BY id;
INSERT INTO geo_multipoint VALUES (1, [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO geo_multipoint VALUES (2, [(5, 5)]);
INSERT INTO geo_multipoint VALUES (3, []);
SELECT mp, toTypeName(mp) FROM geo_multipoint ORDER BY id;
DROP TABLE geo_multipoint;

-- MultiPoint inside the Geometry type.
DROP TABLE IF EXISTS geo_multipoint_geom;
CREATE TABLE geo_multipoint_geom (id Int32, g Geometry) ENGINE = MergeTree ORDER BY id;
INSERT INTO geo_multipoint_geom VALUES (1, [(1, 2), (3, 4)]::MultiPoint);
INSERT INTO geo_multipoint_geom VALUES (2, (5, 6)::Point);
INSERT INTO geo_multipoint_geom VALUES (3, [(1, 1), (2, 2)]::LineString);
SELECT g, variantType(g), toTypeName(g) FROM geo_multipoint_geom ORDER BY id;
SELECT variantElement(g, 'MultiPoint') AS mp, toTypeName(mp) FROM geo_multipoint_geom WHERE variantType(g) = 'MultiPoint' ORDER BY id;
DROP TABLE geo_multipoint_geom;

-- WKT reading and writing. Both MULTIPOINT spellings are valid.
SELECT readWKTMultiPoint('MULTIPOINT (1 1, 2 2, 3 3)') AS mp, toTypeName(mp);
SELECT readWKTMultiPoint('MULTIPOINT ((1 1), (2 2))');
SELECT readWKT('MULTIPOINT (1 1, 2 2)') AS g, variantType(g);
SELECT wkt([(1., 2.), (3., 4.)]::MultiPoint);

-- WKB reading and writing.
SELECT hex(wkb([(1., 1.), (2., 2.)]::MultiPoint));
SELECT readWKBMultiPoint(wkb([(1., 2.), (3., 4.)]::MultiPoint)) AS mp, toTypeName(mp);
SELECT ST_MPointFromWKB(wkb([(5., 6.)]::MultiPoint));
SELECT readWKB(wkb([(1., 2.)]::MultiPoint)) AS g, variantType(g);

-- Geo functions. A set of points has zero area and perimeter.
SELECT areaCartesian([(1., 2.), (3., 4.)]::MultiPoint);
SELECT perimeterCartesian([(1., 2.), (3., 4.)]::MultiPoint);
SELECT areaSpherical([(1., 2.)]::MultiPoint);
SELECT areaCartesian([(1., 2.)]::MultiPoint::Geometry);
SELECT svg([(0., 0.), (10., 10.)]::MultiPoint);
SELECT wkt(polygonConvexHullCartesian([(0., 0.), (5., 0.), (0., 5.), (2., 2.)]::MultiPoint));
SELECT polygonsDistanceCartesian([(0., 0.)]::MultiPoint, [[(4., 3.), (5., 3.), (5., 4.), (4., 3.)]]::Polygon);
SELECT polygonsEqualsCartesian([(1., 2.)]::MultiPoint, [(1., 2.)]::MultiPoint);
SELECT flipCoordinates([(1, 2), (3, 4)]::MultiPoint) AS f, toTypeName(f);

-- MVT encoding.
SELECT MVTEncodeGeom([(13.4, 52.5), (13.5, 52.55)]::MultiPoint, 10, 550, 335) AS g, variantType(g);
SELECT hex(MVTEncode('layer')(MVTEncodeGeom([(13.4, 52.5), (13.5, 52.55)]::MultiPoint, 10, 550, 335)));

-- geometryIntersect accepts MultiPoint, both as a concrete type and through Geometry.
SELECT geometryIntersectCartesian([(1., 2.), (3., 4.)]::MultiPoint, [(3., 4.)]::MultiPoint);
SELECT geometryIntersectCartesian([(1., 2.)]::MultiPoint, [(3., 4.)]::MultiPoint);
SELECT geometryIntersectCartesian([(1., 1.), (9., 9.)]::MultiPoint, [[(0., 0.), (2., 0.), (2., 2.), (0., 2.), (0., 0.)]]::Polygon);
SELECT geometryIntersectCartesian([(1., 1.)]::MultiPoint::Geometry, [[(0., 0.), (2., 0.), (2., 2.), (0., 2.), (0., 0.)]]::Polygon::Geometry);
SELECT geometryIntersectSpherical([(4.36, 50.85)]::MultiPoint, (4.36, 50.85)::Point);

-- Functions with polygon semantics reject MultiPoint like LineString and MultiLineString.
SELECT polygonsIntersectionCartesian([(1., 2.)]::MultiPoint, [(1., 2.)]::MultiPoint); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT polygonsUnionCartesian([(1., 2.)]::MultiPoint, [(1., 2.)]::MultiPoint); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT polygonsSymDifferenceCartesian([(1., 2.)]::MultiPoint, [(1., 2.)]::MultiPoint); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT polygonsWithinCartesian([(1., 2.)]::MultiPoint, [(1., 2.)]::MultiPoint); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT polygonsIntersectCartesian([(1., 2.)]::MultiPoint, [(1., 2.)]::MultiPoint); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
