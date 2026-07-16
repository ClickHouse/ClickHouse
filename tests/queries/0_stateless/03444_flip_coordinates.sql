-- {echo}
SET allow_suspicious_variant_types = 1;

SELECT flipCoordinates(CAST((10.0, 20.0) AS Point));

SELECT flipCoordinates(CAST([(10, 20), (30, 40), (50, 60)] AS LineString));

WITH CAST([[(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]] AS Polygon) AS poly
SELECT flipCoordinates(poly);

WITH CAST([[[(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]]] AS MultiPolygon) AS mpoly
SELECT flipCoordinates(mpoly);

WITH CAST([
    [(0, 0), (100, 0), (100, 100), (0, 100), (0, 0)],
    [(25, 25), (75, 25), (75, 75), (25, 75), (25, 25)]
] AS Polygon) AS poly_with_hole
SELECT flipCoordinates(poly_with_hole);

WITH CAST([[(10, 20), (30, 40)], [(50, 60), (70, 80)]] AS MultiLineString) AS multiline
SELECT flipCoordinates(multiline);

SELECT flipCoordinates(([[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]]::MultiPolygon));

SELECT flipCoordinates((1.23, 4.56)::Point), (([(1.23, 4.56)::Point, (2.34, 5.67)::Point])::Ring);

SELECT flipCoordinates(readWkt('POINT(10 20)'));

SELECT flipCoordinates(readWkt('LINESTRING(10 20, 30 40, 50 60)'));

SELECT flipCoordinates(readWkt('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'));

SELECT flipCoordinates(readWkt('POLYGON((-111.0544 45.0016, -104.0527 45.0016, -104.0527 40.9982, -111.0544 40.9982, -111.0544 45.0016))'));

SELECT flipCoordinates(readWkt('POLYGON((0 0, 100 0, 100 100, 0 100, 0 0), (25 25, 75 25, 75 75, 25 75, 25 25))'));

SELECT flipCoordinates(readWkt('MULTILINESTRING((10 20, 30 40), (50 60, 70 80))'));

SELECT flipCoordinates(readWkt('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)))'));

SELECT flipCoordinates(readWkt('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10)), ((20 20, 50 20, 50 50, 20 50), (30 30, 50 50, 50 30)))'));

SELECT flipCoordinates(readWkt('POINT(-73.935242 40.730610)'));

SELECT flipCoordinates(readWkt('POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))'));

SELECT flipCoordinates(materialize(readWkt('POINT(5 10)'))) FROM numbers(3);

DROP TABLE IF EXISTS test_geom;
CREATE TABLE test_geom (id UInt32, geom Geometry) ENGINE = Memory;
INSERT INTO test_geom VALUES
    (1, readWkt('POINT(10 20)')),
    (2, readWkt('LINESTRING(1 2, 3 4)')),
    (3, readWkt('POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))')),
    (4, readWkt('POINT(30 40)')),
    (5, readWkt('MULTIPOLYGON(((0 0, 2 0, 2 2, 0 2, 0 0)))'));

SELECT id, flipCoordinates(geom) FROM test_geom ORDER BY id;

DROP TABLE test_geom;

-- Issue #110680: flipCoordinates over a Geometry argument must keep the custom `Geometry` type name
-- so the result can be passed directly to functions that require a geometry type.
WITH readWKT('POLYGON((0 0,4 0,4 3,0 3,0 0))') AS g
SELECT toTypeName(g), toTypeName(flipCoordinates(g));

WITH readWKT('POLYGON((0 0,4 0,4 3,0 3,0 0))') AS g
SELECT areaCartesian(flipCoordinates(g));

-- Mixed-type Geometry column: every row keeps the Geometry type.
DROP TABLE IF EXISTS test_geom2;
CREATE TABLE test_geom2 (id UInt32, geom Geometry) ENGINE = Memory;
INSERT INTO test_geom2 VALUES
    (1, readWkt('POINT(10 20)')),
    (2, readWkt('LINESTRING(1 2, 3 4)')),
    (3, readWkt('POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))'));
SELECT id, toTypeName(flipCoordinates(geom)) FROM test_geom2 ORDER BY id;
DROP TABLE test_geom2;

-- Issue #110680 follow-up: a Variant with an empty non-geometry arm must not throw.
-- ColumnVariant keeps an empty subcolumn for every declared alternative; only populated
-- (geometry) arms are flipped, so an unused String arm is left untouched.
SELECT toTypeName(flipCoordinates(CAST(CAST((1.0, 2.0), 'Point') AS Variant(Point, String))));
SELECT flipCoordinates(CAST(CAST((1.0, 2.0), 'Point') AS Variant(Point, String)));

DROP TABLE IF EXISTS test_geom3;
CREATE TABLE test_geom3 (id UInt32, geom Variant(Point, String)) ENGINE = Memory;
INSERT INTO test_geom3 VALUES (1, CAST((10.0, 20.0), 'Point')), (2, CAST((30.0, 40.0), 'Point'));
SELECT id, flipCoordinates(geom), toTypeName(flipCoordinates(geom)) FROM test_geom3 ORDER BY id;
DROP TABLE test_geom3;

-- Issue #110680 follow-up: a populated non-geometry arm must honor variant_throw_on_type_mismatch.
-- With the setting disabled, incompatible rows become NULL (mirroring the default Variant adaptor).
SET variant_throw_on_type_mismatch = 0;
SELECT flipCoordinates(CAST(if(number = 0, CAST((1., 2.), 'Point'), 'x'), 'Variant(Point, String)')) FROM numbers(2);
SELECT flipCoordinates(CAST('x', 'Variant(Point, String)'));
SELECT flipCoordinates(CAST(multiIf(number = 0, CAST((1., 2.), 'Point'), number = 1, 'str', CAST([(3., 4.), (5., 6.)], 'Ring')), 'Variant(Point, Ring, String)')) FROM numbers(3);
-- Valid Geometry rows still flip and keep the type name when the setting is disabled.
SELECT toTypeName(flipCoordinates(readWkt('POINT(10 20)')::Geometry));

-- With the setting enabled (the default), a populated incompatible arm still throws.
SET variant_throw_on_type_mismatch = 1;
SELECT flipCoordinates(CAST(if(number = 0, CAST((1., 2.), 'Point'), 'x'), 'Variant(Point, String)')) FROM numbers(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
