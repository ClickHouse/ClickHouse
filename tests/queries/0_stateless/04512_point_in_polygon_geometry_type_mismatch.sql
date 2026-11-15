-- Test pointInPolygon over a Geometry column that also holds non-polygon-shaped values.
DROP TABLE IF EXISTS pip_geo_mismatch;
CREATE TABLE pip_geo_mismatch (id Int32, geom Geometry) ENGINE = Memory;
INSERT INTO pip_geo_mismatch VALUES (1, readWKT('POINT(1 1)'));
INSERT INTO pip_geo_mismatch VALUES (3, readWKT('POLYGON((0 0, 10 0, 10 10, 0 10))'));
INSERT INTO pip_geo_mismatch VALUES (4, readWKT('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10)), ((20 20, 30 20, 30 30, 20 30)))'));

SELECT '-- default variant_throw_on_type_mismatch=1 : a non-polygon row raises --';
SELECT id, pointInPolygon((5., 5.), geom) FROM pip_geo_mismatch ORDER BY id SETTINGS variant_throw_on_type_mismatch = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- variant_throw_on_type_mismatch=0 : Point -> NULL, polygon-shaped rows matched --';
SELECT id, pointInPolygon((5., 5.), geom) FROM pip_geo_mismatch ORDER BY id SETTINGS variant_throw_on_type_mismatch = 0;

SELECT '-- point inside the second polygon of the MultiPolygon row --';
SELECT id, pointInPolygon((25., 25.), geom) FROM pip_geo_mismatch ORDER BY id SETTINGS variant_throw_on_type_mismatch = 0;

DROP TABLE pip_geo_mismatch;
