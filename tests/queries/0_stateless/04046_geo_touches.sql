-- Tags: no-fasttest

SELECT 'geoTouchesCartesian: point on polygon boundary vertex';
SELECT geoTouchesCartesian(
    CAST((0.0, 0.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoTouchesCartesian: point on polygon edge';
SELECT geoTouchesCartesian(
    CAST((0.005, 0.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoTouchesCartesian: point inside polygon';
SELECT geoTouchesCartesian(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoTouchesCartesian: point outside polygon';
SELECT geoTouchesCartesian(
    CAST((5.0, 5.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoTouchesCartesian: two polygons sharing edge';
SELECT geoTouchesCartesian(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.01, 0.0), (0.02, 0.0), (0.02, 0.01), (0.01, 0.01), (0.01, 0.0)]], 'Polygon'));

SELECT 'geoTouchesCartesian: two overlapping polygons';
SELECT geoTouchesCartesian(
    CAST([[(0.0, 0.0), (0.02, 0.0), (0.02, 0.02), (0.0, 0.02), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.01, 0.01), (0.03, 0.01), (0.03, 0.03), (0.01, 0.03), (0.01, 0.01)]], 'Polygon'));

SELECT 'geoTouchesCartesian: point-point same (never touches)';
SELECT geoTouchesCartesian(
    CAST((1.0, 2.0), 'Point'),
    CAST((1.0, 2.0), 'Point'));

SELECT 'geoTouchesCartesian: disjoint polygons';
SELECT geoTouchesCartesian(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST([[(1.0, 1.0), (1.01, 1.0), (1.01, 1.01), (1.0, 1.01), (1.0, 1.0)]], 'Polygon'));

SELECT 'geoTouchesSpherical: point on polygon boundary vertex';
SELECT geoTouchesSpherical(
    CAST((0.0, 0.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoTouchesSpherical: point inside polygon';
SELECT geoTouchesSpherical(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoTouchesSpherical: two polygons sharing edge';
SELECT geoTouchesSpherical(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.01, 0.0), (0.02, 0.0), (0.02, 0.01), (0.01, 0.01), (0.01, 0.0)]], 'Polygon'));

SELECT 'ST_Touches is alias for geoTouchesCartesian';
SELECT
    ST_Touches(CAST((0.0, 0.0), 'Point'), CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'))
    = geoTouchesCartesian(CAST((0.0, 0.0), 'Point'), CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));
