-- Tags: no-fasttest

SELECT 'point on polygon boundary vertex';
SELECT ST_Touches(
    CAST((0.0, 0.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'point on polygon edge';
SELECT ST_Touches(
    CAST((0.005, 0.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'point inside polygon';
SELECT ST_Touches(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'point outside polygon';
SELECT ST_Touches(
    CAST((5.0, 5.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'two polygons sharing edge';
SELECT ST_Touches(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.01, 0.0), (0.02, 0.0), (0.02, 0.01), (0.01, 0.01), (0.01, 0.0)]], 'Polygon'));

SELECT 'two overlapping polygons';
SELECT ST_Touches(
    CAST([[(0.0, 0.0), (0.02, 0.0), (0.02, 0.02), (0.0, 0.02), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.01, 0.01), (0.03, 0.01), (0.03, 0.03), (0.01, 0.03), (0.01, 0.01)]], 'Polygon'));

SELECT 'point-point same';
SELECT ST_Touches(
    CAST((1.0, 2.0), 'Point'),
    CAST((1.0, 2.0), 'Point'));

SELECT 'disjoint polygons';
SELECT ST_Touches(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST([[(1.0, 1.0), (1.01, 1.0), (1.01, 1.01), (1.0, 1.01), (1.0, 1.0)]], 'Polygon'));
