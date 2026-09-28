-- Tags: no-fasttest

SELECT 'geoContainsCartesian: polygon contains interior point';
SELECT geoContainsCartesian(
    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'),
    CAST((5.0, 5.0), 'Point'));

SELECT 'geoContainsCartesian: polygon not contains exterior point';
SELECT geoContainsCartesian(
    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'),
    CAST((15.0, 15.0), 'Point'));

SELECT 'geoContainsCartesian: point cannot contain polygon';
SELECT geoContainsCartesian(
    CAST((5.0, 5.0), 'Point'),
    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoContainsCartesian: big polygon contains small polygon';
SELECT geoContainsCartesian(
    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'),
    CAST([[(2.0, 2.0), (3.0, 2.0), (3.0, 3.0), (2.0, 3.0), (2.0, 2.0)]], 'Polygon'));

SELECT 'geoContainsCartesian: small polygon not contains big';
SELECT geoContainsCartesian(
    CAST([[(2.0, 2.0), (3.0, 2.0), (3.0, 3.0), (2.0, 3.0), (2.0, 2.0)]], 'Polygon'),
    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoContainsCartesian: ST_Contains is alias for geoContainsCartesian';
SELECT
    geoContainsCartesian(CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'), CAST((5.0, 5.0), 'Point'))
    =
    ST_Contains(CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'), CAST((5.0, 5.0), 'Point'));

SELECT 'geoContainsSpherical: polygon contains interior point';
SELECT geoContainsSpherical(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST((0.005, 0.005), 'Point'));

SELECT 'geoContainsSpherical: polygon not contains exterior point';
SELECT geoContainsSpherical(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST((5.0, 5.0), 'Point'));

SELECT 'geoContainsSpherical: point cannot contain polygon';
SELECT geoContainsSpherical(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoContainsSpherical: big polygon contains small polygon';
SELECT geoContainsSpherical(
    CAST([[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.1, 0.1), (0.2, 0.1), (0.2, 0.2), (0.1, 0.2), (0.1, 0.1)]], 'Polygon'));

SELECT 'geoContainsSpherical: const polygon, variable points';
SELECT geoContainsSpherical(
    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'),
    CAST(p, 'Point'))
FROM VALUES('p Tuple(Float64, Float64)',
    (5.0, 5.0), (15.0, 5.0), (0.5, 0.5));
