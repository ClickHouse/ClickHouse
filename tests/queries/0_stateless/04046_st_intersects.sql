-- Tags: no-fasttest

SELECT 'point inside polygon';
SELECT ST_Intersects(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'point outside polygon';
SELECT ST_Intersects(
    CAST((5.0, 5.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'point-point same';
SELECT ST_Intersects(
    CAST((1.0, 2.0), 'Point'),
    CAST((1.0, 2.0), 'Point'));

SELECT 'point-point different';
SELECT ST_Intersects(
    CAST((1.0, 2.0), 'Point'),
    CAST((3.0, 4.0), 'Point'));

SELECT 'point in multipolygon';
SELECT ST_Intersects(
    CAST((0.005, 0.005), 'Point'),
    CAST([[[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], [[(1.0, 1.0), (1.01, 1.0), (1.01, 1.01), (1.0, 1.01), (1.0, 1.0)]]], 'MultiPolygon'));

SELECT 'polygon-polygon overlapping';
SELECT ST_Intersects(
    CAST([[(0.0, 0.0), (0.02, 0.0), (0.02, 0.02), (0.0, 0.02), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.01, 0.01), (0.03, 0.01), (0.03, 0.03), (0.01, 0.03), (0.01, 0.01)]], 'Polygon'));

SELECT 'polygon-polygon non-overlapping';
SELECT ST_Intersects(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST([[(1.0, 1.0), (1.01, 1.0), (1.01, 1.01), (1.0, 1.01), (1.0, 1.0)]], 'Polygon'));

SELECT 'point in ring';
SELECT ST_Intersects(
    CAST((0.005, 0.005), 'Point'),
    CAST([(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)], 'Ring'));

SELECT 'polygon with hole - point in hole';
SELECT ST_Intersects(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)], [(0.003, 0.003), (0.007, 0.003), (0.007, 0.007), (0.003, 0.007), (0.003, 0.003)]], 'Polygon'));

SELECT 'const polygon, variable points';
SELECT ST_Intersects(
    CAST(p, 'Point'),
    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
FROM VALUES('p Tuple(Float64, Float64)',
    (5.0, 5.0), (15.0, 5.0));
