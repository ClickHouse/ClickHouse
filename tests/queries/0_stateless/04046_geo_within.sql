-- Tags: no-fasttest

SELECT 'geoWithinCartesian: point within polygon';
SELECT geoWithinCartesian(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoWithinCartesian: point not within polygon';
SELECT geoWithinCartesian(
    CAST((5.0, 5.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoWithinCartesian: polygon within point is always false';
SELECT geoWithinCartesian(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST((0.005, 0.005), 'Point'));

SELECT 'geoWithinCartesian: same point';
SELECT geoWithinCartesian(
    CAST((1.0, 2.0), 'Point'),
    CAST((1.0, 2.0), 'Point'));

SELECT 'geoWithinCartesian: point in hole';
SELECT geoWithinCartesian(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)], [(0.003, 0.003), (0.007, 0.003), (0.007, 0.007), (0.003, 0.007), (0.003, 0.003)]], 'Polygon'));

SELECT 'geoWithinCartesian: small polygon within big';
SELECT geoWithinCartesian(
    CAST([[(0.1, 0.1), (0.2, 0.1), (0.2, 0.2), (0.1, 0.2), (0.1, 0.1)]], 'Polygon'),
    CAST([[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoWithinCartesian: big not within small';
SELECT geoWithinCartesian(
    CAST([[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.1, 0.1), (0.2, 0.1), (0.2, 0.2), (0.1, 0.2), (0.1, 0.1)]], 'Polygon'));

SELECT 'geoWithinCartesian: const polygon, variable points';
SELECT geoWithinCartesian(
    CAST(p, 'Point'),
    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
FROM VALUES('p Tuple(Float64, Float64)',
    (5.0, 5.0), (15.0, 5.0), (0.5, 0.5), (-1.0, -1.0), (9.9, 9.9));

SELECT 'geoWithinCartesian: const point, variable polygons';
SELECT geoWithinCartesian(
    CAST((5.0, 5.0), 'Point'),
    CAST(poly, 'Polygon'))
FROM VALUES('poly Array(Array(Tuple(Float64, Float64)))',
    ([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]]),
    ([[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]]));

SELECT 'geoWithinSpherical: point within polygon';
SELECT geoWithinSpherical(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoWithinSpherical: point not within polygon';
SELECT geoWithinSpherical(
    CAST((5.0, 5.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoWithinSpherical: small polygon within big';
SELECT geoWithinSpherical(
    CAST([[(0.1, 0.1), (0.2, 0.1), (0.2, 0.2), (0.1, 0.2), (0.1, 0.1)]], 'Polygon'),
    CAST([[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]], 'Polygon'));

SELECT 'ST_Within is alias for geoWithinCartesian';
SELECT
    ST_Within(CAST((0.005, 0.005), 'Point'), CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'))
    = geoWithinCartesian(CAST((0.005, 0.005), 'Point'), CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'symmetry: geoWithinCartesian(A,B) == geoContainsCartesian(B,A)';
SELECT
    geoWithinCartesian(CAST((0.005, 0.005), 'Point'), CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'))
    = geoContainsCartesian(CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'), CAST((0.005, 0.005), 'Point'));
