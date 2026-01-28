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
