-- Tags: no-fasttest

SELECT 'geoContainsCartesian: polygon contains interior point';
SELECT geoContainsCartesian(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST((0.005, 0.005), 'Point'));

SELECT 'geoContainsCartesian: polygon not contains exterior point';
SELECT geoContainsCartesian(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST((5.0, 5.0), 'Point'));

SELECT 'geoContainsCartesian: point cannot contain polygon';
SELECT geoContainsCartesian(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoContainsCartesian: point contains same point';
SELECT geoContainsCartesian(
    CAST((1.0, 2.0), 'Point'),
    CAST((1.0, 2.0), 'Point'));

SELECT 'geoContainsCartesian: point not contains different point';
SELECT geoContainsCartesian(
    CAST((1.0, 2.0), 'Point'),
    CAST((3.0, 4.0), 'Point'));

SELECT 'geoContainsCartesian: polygon with hole - point in hole';
SELECT geoContainsCartesian(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)], [(0.003, 0.003), (0.007, 0.003), (0.007, 0.007), (0.003, 0.007), (0.003, 0.003)]], 'Polygon'),
    CAST((0.005, 0.005), 'Point'));

SELECT 'geoContainsCartesian: big polygon contains small polygon';
SELECT geoContainsCartesian(
    CAST([[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.1, 0.1), (0.2, 0.1), (0.2, 0.2), (0.1, 0.2), (0.1, 0.1)]], 'Polygon'));

SELECT 'geoContainsCartesian: small polygon not contains big';
SELECT geoContainsCartesian(
    CAST([[(0.1, 0.1), (0.2, 0.1), (0.2, 0.2), (0.1, 0.2), (0.1, 0.1)]], 'Polygon'),
    CAST([[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoContainsCartesian: multipolygon contains point';
SELECT geoContainsCartesian(
    CAST([[[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], [[(1.0, 1.0), (1.01, 1.0), (1.01, 1.01), (1.0, 1.01), (1.0, 1.0)]]], 'MultiPolygon'),
    CAST((0.005, 0.005), 'Point'));

SELECT 'geoContainsCartesian: const polygon, variable points';
SELECT geoContainsCartesian(
    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'),
    CAST(p, 'Point'))
FROM VALUES('p Tuple(Float64, Float64)',
    (5.0, 5.0), (15.0, 5.0), (0.5, 0.5));

SELECT 'geoContainsSpherical: polygon contains interior point';
SELECT geoContainsSpherical(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST((0.005, 0.005), 'Point'));

SELECT 'geoContainsSpherical: polygon not contains exterior point';
SELECT geoContainsSpherical(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST((5.0, 5.0), 'Point'));

SELECT 'geoContainsSpherical: big polygon contains small polygon';
SELECT geoContainsSpherical(
    CAST([[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.1, 0.1), (0.2, 0.1), (0.2, 0.2), (0.1, 0.2), (0.1, 0.1)]], 'Polygon'));

SELECT 'ST_Contains is alias for geoContainsCartesian';
SELECT
    ST_Contains(CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'), CAST((0.005, 0.005), 'Point'))
    = geoContainsCartesian(CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'), CAST((0.005, 0.005), 'Point'));
