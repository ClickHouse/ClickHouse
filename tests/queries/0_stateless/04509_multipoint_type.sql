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

-- flipCoordinates works structurally and preserves the type.
SELECT flipCoordinates([(1, 2), (3, 4)]::MultiPoint) AS flipped, toTypeName(flipped);

-- Geo functions are not implemented for MultiPoint yet.
SELECT wkt([(1., 2.)]::MultiPoint); -- { serverError NOT_IMPLEMENTED }
SELECT svg([(1., 2.)]::MultiPoint); -- { serverError NOT_IMPLEMENTED }
SELECT areaCartesian([(1., 2.)]::MultiPoint); -- { serverError NOT_IMPLEMENTED }
SELECT areaSpherical([(1., 2.)]::MultiPoint); -- { serverError NOT_IMPLEMENTED }
SELECT perimeterCartesian([(1., 2.)]::MultiPoint); -- { serverError NOT_IMPLEMENTED }
SELECT areaCartesian([(1., 2.)]::MultiPoint::Geometry); -- { serverError NOT_IMPLEMENTED }
SELECT polygonsIntersectCartesian([(1., 2.)]::MultiPoint, [(1., 2.)]::MultiPoint); -- { serverError NOT_IMPLEMENTED }

-- Reading MultiPoint WKT and WKB values is not implemented yet.
SELECT readWKT('MULTIPOINT (1 1, 2 2)'); -- { serverError NOT_IMPLEMENTED }
SELECT readWKB(unhex('0104000000010000000101000000000000000000f03f000000000000f03f')); -- { serverError NOT_IMPLEMENTED }

-- MVT encoding is not implemented for MultiPoint yet.
SELECT MVTEncodeGeom([(13.4, 52.5)]::MultiPoint::Geometry, 10, 550, 335); -- { serverError NOT_IMPLEMENTED }
SELECT MVTEncodeGeom([(13.4, 52.5)]::MultiPoint, 10, 550, 335); -- { serverError NOT_IMPLEMENTED }
SELECT MVTEncode('t')([(13.4, 52.5)]::MultiPoint::Geometry); -- { serverError NOT_IMPLEMENTED }

-- The GeoJSON output format does not support MultiPoint yet, neither as a concrete
-- geometry column nor as a value inside a Geometry column.
SELECT formatRow('GeoJSON', [(1., 2.)]::MultiPoint); -- { serverError NOT_IMPLEMENTED }
SELECT formatRow('GeoJSON', [(1., 2.)]::MultiPoint::Geometry); -- { serverError NOT_IMPLEMENTED }
