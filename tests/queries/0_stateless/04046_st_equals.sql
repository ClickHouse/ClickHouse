-- Tags: no-fasttest

SELECT 'same polygon';
SELECT ST_Equals(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'different polygons';
SELECT ST_Equals(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST([[(1.0, 1.0), (1.01, 1.0), (1.01, 1.01), (1.0, 1.01), (1.0, 1.0)]], 'Polygon'));

SELECT 'same point';
SELECT ST_Equals(
    CAST((1.0, 2.0), 'Point'),
    CAST((1.0, 2.0), 'Point'));

SELECT 'different points';
SELECT ST_Equals(
    CAST((1.0, 2.0), 'Point'),
    CAST((3.0, 4.0), 'Point'));

SELECT 'point vs polygon';
SELECT ST_Equals(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'same polygon different vertex order';
SELECT ST_Equals(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0), (0.01, 0.0)]], 'Polygon'));

SELECT 'big contains small not equal';
SELECT ST_Equals(
    CAST([[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.1, 0.1), (0.2, 0.1), (0.2, 0.2), (0.1, 0.2), (0.1, 0.1)]], 'Polygon'));
