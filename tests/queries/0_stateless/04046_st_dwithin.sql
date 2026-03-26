-- Tags: no-fasttest

SELECT 'close points ~111m threshold 200m';
SELECT ST_DWithin(
    CAST((0.0, 0.0), 'Point'),
    CAST((0.001, 0.0), 'Point'),
    200.0);

SELECT 'far points ~111km threshold 1000m';
SELECT ST_DWithin(
    CAST((0.0, 0.0), 'Point'),
    CAST((1.0, 0.0), 'Point'),
    1000.0);

SELECT 'same point threshold 0';
SELECT ST_DWithin(
    CAST((1.0, 2.0), 'Point'),
    CAST((1.0, 2.0), 'Point'),
    0.0);

SELECT 'point inside polygon threshold 0';
SELECT ST_DWithin(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    0.0);

SELECT 'point far from polygon threshold 1km';
SELECT ST_DWithin(
    CAST((5.0, 5.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    1000.0);

SELECT 'point far from polygon huge threshold';
SELECT ST_DWithin(
    CAST((5.0, 5.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    1000000.0);
