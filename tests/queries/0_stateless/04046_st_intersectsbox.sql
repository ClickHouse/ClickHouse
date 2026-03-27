-- Tags: no-fasttest

SELECT 'point inside box';
SELECT ST_IntersectsBox(
    CAST((5.0, 5.0), 'Point'),
    0.0, 0.0, 10.0, 10.0);

SELECT 'point outside box';
SELECT ST_IntersectsBox(
    CAST((50.0, 50.0), 'Point'),
    0.0, 0.0, 10.0, 10.0);

SELECT 'point on boundary';
SELECT ST_IntersectsBox(
    CAST((0.0, 0.0), 'Point'),
    0.0, 0.0, 10.0, 10.0);

SELECT 'polygon overlapping box';
SELECT ST_IntersectsBox(
    CAST([[(5.0, 5.0), (15.0, 5.0), (15.0, 15.0), (5.0, 15.0), (5.0, 5.0)]], 'Polygon'),
    0.0, 0.0, 10.0, 10.0);

SELECT 'polygon inside box';
SELECT ST_IntersectsBox(
    CAST([[(2.0, 2.0), (3.0, 2.0), (3.0, 3.0), (2.0, 3.0), (2.0, 2.0)]], 'Polygon'),
    0.0, 0.0, 10.0, 10.0);

SELECT 'polygon outside box';
SELECT ST_IntersectsBox(
    CAST([[(20.0, 20.0), (21.0, 20.0), (21.0, 21.0), (20.0, 21.0), (20.0, 20.0)]], 'Polygon'),
    0.0, 0.0, 10.0, 10.0);

SELECT 'multipolygon intersecting box';
SELECT ST_IntersectsBox(
    CAST([[[(5.0, 5.0), (6.0, 5.0), (6.0, 6.0), (5.0, 6.0), (5.0, 5.0)]], [[(50.0, 50.0), (51.0, 50.0), (51.0, 51.0), (50.0, 51.0), (50.0, 50.0)]]], 'MultiPolygon'),
    0.0, 0.0, 10.0, 10.0);

SELECT 'inverted xmin/xmax';
SELECT ST_IntersectsBox(
    CAST((5.0, 5.0), 'Point'),
    10.0, 0.0, 0.0, 10.0); -- { serverError BAD_ARGUMENTS }

SELECT 'inverted ymin/ymax';
SELECT ST_IntersectsBox(
    CAST((5.0, 5.0), 'Point'),
    0.0, 10.0, 10.0, 0.0); -- { serverError BAD_ARGUMENTS }
