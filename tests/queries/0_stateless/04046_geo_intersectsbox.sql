-- Tags: no-fasttest

SELECT '--- geoIntersectsBoxSpherical ---';

SELECT 'point inside box';
SELECT geoIntersectsBoxSpherical(
    CAST((5.0, 5.0), 'Point'),
    0.0, 0.0, 10.0, 10.0);

SELECT 'point outside box';
SELECT geoIntersectsBoxSpherical(
    CAST((50.0, 50.0), 'Point'),
    0.0, 0.0, 10.0, 10.0);

SELECT 'point on boundary';
SELECT geoIntersectsBoxSpherical(
    CAST((0.0, 0.0), 'Point'),
    0.0, 0.0, 10.0, 10.0);

SELECT 'polygon overlapping box';
SELECT geoIntersectsBoxSpherical(
    CAST([[(5.0, 5.0), (15.0, 5.0), (15.0, 15.0), (5.0, 15.0), (5.0, 5.0)]], 'Polygon'),
    0.0, 0.0, 10.0, 10.0);

SELECT 'polygon inside box';
SELECT geoIntersectsBoxSpherical(
    CAST([[(2.0, 2.0), (3.0, 2.0), (3.0, 3.0), (2.0, 3.0), (2.0, 2.0)]], 'Polygon'),
    0.0, 0.0, 10.0, 10.0);

SELECT 'polygon outside box';
SELECT geoIntersectsBoxSpherical(
    CAST([[(20.0, 20.0), (21.0, 20.0), (21.0, 21.0), (20.0, 21.0), (20.0, 20.0)]], 'Polygon'),
    0.0, 0.0, 10.0, 10.0);

SELECT 'multipolygon intersecting box';
SELECT geoIntersectsBoxSpherical(
    CAST([[[(5.0, 5.0), (6.0, 5.0), (6.0, 6.0), (5.0, 6.0), (5.0, 5.0)]], [[(50.0, 50.0), (51.0, 50.0), (51.0, 51.0), (50.0, 51.0), (50.0, 50.0)]]], 'MultiPolygon'),
    0.0, 0.0, 10.0, 10.0);

SELECT 'wrapped longitude xmin/xmax';
SELECT geoIntersectsBoxSpherical(
    CAST((5.0, 5.0), 'Point'),
    10.0, 0.0, 0.0, 10.0);

SELECT 'wrapped longitude intersects';
SELECT geoIntersectsBoxSpherical(
    CAST((170.0, 5.0), 'Point'),
    10.0, 0.0, 0.0, 10.0);

SELECT 'inverted ymin/ymax';
SELECT geoIntersectsBoxSpherical(
    CAST((5.0, 5.0), 'Point'),
    0.0, 10.0, 10.0, 0.0); -- { serverError BAD_ARGUMENTS }

SELECT '--- geoIntersectsBoxCartesian ---';

SELECT 'point inside box';
SELECT geoIntersectsBoxCartesian(
    CAST((5.0, 5.0), 'Point'),
    0.0, 0.0, 10.0, 10.0);

SELECT 'point outside box';
SELECT geoIntersectsBoxCartesian(
    CAST((50.0, 50.0), 'Point'),
    0.0, 0.0, 10.0, 10.0);

SELECT 'polygon overlapping box';
SELECT geoIntersectsBoxCartesian(
    CAST([[(5.0, 5.0), (15.0, 5.0), (15.0, 15.0), (5.0, 15.0), (5.0, 5.0)]], 'Polygon'),
    0.0, 0.0, 10.0, 10.0);

SELECT '--- ST_IntersectsBox with st_function_use_spherical ---';

SET st_function_use_spherical = true;
SELECT ST_IntersectsBox(
    CAST((5.0, 5.0), 'Point'),
    0.0, 0.0, 10.0, 10.0)
    = geoIntersectsBoxSpherical(
    CAST((5.0, 5.0), 'Point'),
    0.0, 0.0, 10.0, 10.0);

SET st_function_use_spherical = false;
SELECT ST_IntersectsBox(
    CAST((5.0, 5.0), 'Point'),
    0.0, 0.0, 10.0, 10.0)
    = geoIntersectsBoxCartesian(
    CAST((5.0, 5.0), 'Point'),
    0.0, 0.0, 10.0, 10.0);
