-- Tags: no-fasttest

SELECT 'point within polygon';
SELECT ST_Within(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'point not within polygon';
SELECT ST_Within(
    CAST((5.0, 5.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'polygon within point';
SELECT ST_Within(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST((0.005, 0.005), 'Point'));

SELECT 'same point';
SELECT ST_Within(
    CAST((1.0, 2.0), 'Point'),
    CAST((1.0, 2.0), 'Point'));

SELECT 'point in hole';
SELECT ST_Within(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)], [(0.003, 0.003), (0.007, 0.003), (0.007, 0.007), (0.003, 0.007), (0.003, 0.003)]], 'Polygon'));

SELECT 'small polygon within big';
SELECT ST_Within(
    CAST([[(0.1, 0.1), (0.2, 0.1), (0.2, 0.2), (0.1, 0.2), (0.1, 0.1)]], 'Polygon'),
    CAST([[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]], 'Polygon'));

SELECT 'big not within small';
SELECT ST_Within(
    CAST([[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.1, 0.1), (0.2, 0.1), (0.2, 0.2), (0.1, 0.2), (0.1, 0.1)]], 'Polygon'));

SELECT 'symmetry: ST_Within(A,B) == ST_Contains(B,A)';
SELECT ST_Within(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'))
=
ST_Contains(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST((0.005, 0.005), 'Point'));
