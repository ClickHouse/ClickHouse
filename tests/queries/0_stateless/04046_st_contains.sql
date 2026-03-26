-- Tags: no-fasttest

SELECT 'polygon contains interior point';
SELECT ST_Contains(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST((0.005, 0.005), 'Point'));

SELECT 'polygon not contains exterior point';
SELECT ST_Contains(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST((5.0, 5.0), 'Point'));

SELECT 'point cannot contain polygon';
SELECT ST_Contains(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'point contains same point';
SELECT ST_Contains(
    CAST((1.0, 2.0), 'Point'),
    CAST((1.0, 2.0), 'Point'));

SELECT 'point not contains different point';
SELECT ST_Contains(
    CAST((1.0, 2.0), 'Point'),
    CAST((3.0, 4.0), 'Point'));

SELECT 'polygon with hole - point in hole';
SELECT ST_Contains(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)], [(0.003, 0.003), (0.007, 0.003), (0.007, 0.007), (0.003, 0.007), (0.003, 0.003)]], 'Polygon'),
    CAST((0.005, 0.005), 'Point'));

SELECT 'big polygon contains small polygon';
SELECT ST_Contains(
    CAST([[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.1, 0.1), (0.2, 0.1), (0.2, 0.2), (0.1, 0.2), (0.1, 0.1)]], 'Polygon'));

SELECT 'small polygon not contains big';
SELECT ST_Contains(
    CAST([[(0.1, 0.1), (0.2, 0.1), (0.2, 0.2), (0.1, 0.2), (0.1, 0.1)]], 'Polygon'),
    CAST([[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]], 'Polygon'));

SELECT 'multipolygon contains point';
SELECT ST_Contains(
    CAST([[[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], [[(1.0, 1.0), (1.01, 1.0), (1.01, 1.01), (1.0, 1.01), (1.0, 1.0)]]], 'MultiPolygon'),
    CAST((0.005, 0.005), 'Point'));
