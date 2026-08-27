-- `recursiveRemoveLowCardinality` must preserve custom type names
-- (`LineString` vs `Ring`, `MultiLineString` vs `Polygon`).

SELECT 'LineString (no area) = 0';
SELECT areaCartesian(CAST([(0., 0.), (4., 0.), (4., 4.), (0., 4.)], 'LineString'));

SELECT 'LineString::Geometry (no area) = 0';
SELECT areaCartesian(CAST([(0., 0.), (4., 0.), (4., 4.), (0., 4.)], 'LineString')::Geometry);

SELECT 'Ring (closed polygon) = -16';
SELECT areaCartesian(CAST([(0., 0.), (4., 0.), (4., 4.), (0., 4.)], 'Ring'));

SELECT 'LineString from table column = 0';
DROP TABLE IF EXISTS t_geometry_cast_custom_type_name;
CREATE TABLE t_geometry_cast_custom_type_name (ls LineString) ENGINE = Memory;
INSERT INTO t_geometry_cast_custom_type_name VALUES ([(0., 0.), (4., 0.), (4., 4.), (0., 4.)]);
SELECT areaCartesian(ls) FROM t_geometry_cast_custom_type_name;
DROP TABLE t_geometry_cast_custom_type_name;

SELECT 'MultiLineString (no area) = 0';
SELECT areaCartesian(CAST([[(0., 0.), (0., 5.), (5., 5.), (5., 0.)]], 'MultiLineString'));

SELECT 'Polygon (5x5 square) = 25';
SELECT areaCartesian(CAST([[(0., 0.), (0., 5.), (5., 5.), (5., 0.)]], 'Polygon'));

SELECT 'MultiLineString from table column = 0';
DROP TABLE IF EXISTS t_geometry_cast_custom_type_name_mls;
CREATE TABLE t_geometry_cast_custom_type_name_mls (mls MultiLineString) ENGINE = Memory;
INSERT INTO t_geometry_cast_custom_type_name_mls VALUES ([[(0., 0.), (0., 5.), (5., 5.), (5., 0.)]]);
SELECT areaCartesian(mls) FROM t_geometry_cast_custom_type_name_mls;
DROP TABLE t_geometry_cast_custom_type_name_mls;

SELECT 'Polygon from table column = 25';
DROP TABLE IF EXISTS t_geometry_cast_custom_type_name_poly;
CREATE TABLE t_geometry_cast_custom_type_name_poly (p Polygon) ENGINE = Memory;
INSERT INTO t_geometry_cast_custom_type_name_poly VALUES ([[(0., 0.), (0., 5.), (5., 5.), (5., 0.)]]);
SELECT areaCartesian(p) FROM t_geometry_cast_custom_type_name_poly;
DROP TABLE t_geometry_cast_custom_type_name_poly;

-- Geometry (Variant) type path
SELECT 'Geometry from LineString = 0';
SELECT areaCartesian(CAST([(0., 0.), (4., 0.), (4., 4.), (0., 4.)], 'LineString')::Geometry);

SELECT 'Geometry from Ring = -16';
SELECT areaCartesian(CAST([(0., 0.), (4., 0.), (4., 4.), (0., 4.)], 'Ring')::Geometry);

SELECT 'Geometry from Polygon = 25';
SELECT areaCartesian(CAST([[(0., 0.), (0., 5.), (5., 5.), (5., 0.)]], 'Polygon')::Geometry);

SELECT 'Geometry from MultiLineString = 0';
SELECT areaCartesian(CAST([[(0., 0.), (0., 5.), (5., 5.), (5., 0.)]], 'MultiLineString')::Geometry);

SELECT 'Geometry column from table = 25';
DROP TABLE IF EXISTS t_geometry_cast_custom_type_name_geom;
CREATE TABLE t_geometry_cast_custom_type_name_geom (g Geometry) ENGINE = Memory;
INSERT INTO t_geometry_cast_custom_type_name_geom VALUES (CAST([[(0., 0.), (0., 5.), (5., 5.), (5., 0.)]], 'Polygon'));
SELECT areaCartesian(g) FROM t_geometry_cast_custom_type_name_geom;
DROP TABLE t_geometry_cast_custom_type_name_geom;

-- flipCoordinates on Geometry exercises the default FunctionBaseVariantAdaptor path
-- (flipCoordinates does not override useDefaultImplementationForVariant).
-- This verifies that the Variant adaptor preserves all 6 custom-named geometry types
-- instead of deduplicating to 4 raw types. See https://github.com/ClickHouse/ClickHouse/issues/103207
SELECT 'flipCoordinates on Geometry round-trip';
SET allow_suspicious_variant_types = 1;
DROP TABLE IF EXISTS t_flip_src;
DROP TABLE IF EXISTS t_flip_dst;
CREATE TABLE t_flip_src (id UInt32, geom Geometry) ENGINE = Memory;
CREATE TABLE t_flip_dst (id UInt32, geom Geometry) ENGINE = Memory;
INSERT INTO t_flip_src VALUES
    (1, readWkt('POINT(10 20)')),
    (2, readWkt('LINESTRING(1 2, 3 4)')),
    (3, readWkt('POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))')),
    (4, readWkt('MULTIPOLYGON(((0 0, 2 0, 2 2, 0 2, 0 0)))'));
INSERT INTO t_flip_dst SELECT id, flipCoordinates(geom) FROM t_flip_src;
SELECT id, geom FROM t_flip_dst ORDER BY id;
DROP TABLE t_flip_src;
DROP TABLE t_flip_dst;
