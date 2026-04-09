-- Tags: no-fasttest

SELECT 'geoIntersectsCartesian: point inside polygon';
SELECT geoIntersectsCartesian(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoIntersectsCartesian: point outside polygon';
SELECT geoIntersectsCartesian(
    CAST((5.0, 5.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoIntersectsCartesian: point-point same';
SELECT geoIntersectsCartesian(
    CAST((1.0, 2.0), 'Point'),
    CAST((1.0, 2.0), 'Point'));

SELECT 'geoIntersectsCartesian: point-point different';
SELECT geoIntersectsCartesian(
    CAST((1.0, 2.0), 'Point'),
    CAST((3.0, 4.0), 'Point'));

SELECT 'geoIntersectsCartesian: point in multipolygon';
SELECT geoIntersectsCartesian(
    CAST((0.005, 0.005), 'Point'),
    CAST([[[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], [[(1.0, 1.0), (1.01, 1.0), (1.01, 1.01), (1.0, 1.01), (1.0, 1.0)]]], 'MultiPolygon'));

SELECT 'geoIntersectsCartesian: polygon-polygon overlapping';
SELECT geoIntersectsCartesian(
    CAST([[(0.0, 0.0), (0.02, 0.0), (0.02, 0.02), (0.0, 0.02), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.01, 0.01), (0.03, 0.01), (0.03, 0.03), (0.01, 0.03), (0.01, 0.01)]], 'Polygon'));

SELECT 'geoIntersectsCartesian: polygon-polygon non-overlapping';
SELECT geoIntersectsCartesian(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST([[(1.0, 1.0), (1.01, 1.0), (1.01, 1.01), (1.0, 1.01), (1.0, 1.0)]], 'Polygon'));

SELECT 'geoIntersectsCartesian: polygon with hole - point in hole';
SELECT geoIntersectsCartesian(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)], [(0.003, 0.003), (0.007, 0.003), (0.007, 0.007), (0.003, 0.007), (0.003, 0.003)]], 'Polygon'));

SELECT 'geoIntersectsCartesian: const polygon, variable points';
SELECT geoIntersectsCartesian(
    CAST(p, 'Point'),
    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
FROM VALUES('p Tuple(Float64, Float64)',
    (5.0, 5.0), (15.0, 5.0));

SELECT 'geoIntersectsSpherical: point inside polygon';
SELECT geoIntersectsSpherical(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoIntersectsSpherical: polygon-polygon overlapping';
SELECT geoIntersectsSpherical(
    CAST([[(0.0, 0.0), (0.02, 0.0), (0.02, 0.02), (0.0, 0.02), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.01, 0.01), (0.03, 0.01), (0.03, 0.03), (0.01, 0.03), (0.01, 0.01)]], 'Polygon'));

SELECT 'geoIntersectsSpherical: polygon-polygon non-overlapping';
SELECT geoIntersectsSpherical(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST([[(1.0, 1.0), (1.01, 1.0), (1.01, 1.01), (1.0, 1.01), (1.0, 1.0)]], 'Polygon'));

SELECT 'ST_Intersects is alias for geoIntersectsCartesian';
SELECT
    ST_Intersects(CAST((0.005, 0.005), 'Point'), CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'))
    = geoIntersectsCartesian(CAST((0.005, 0.005), 'Point'), CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));
