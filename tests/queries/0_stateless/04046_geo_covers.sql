-- Tags: no-fasttest

SELECT 'geoCoversCartesian: interior point';
SELECT geoCoversCartesian(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST((0.005, 0.005), 'Point'));

SELECT 'geoCoversCartesian: boundary point covered';
SELECT geoCoversCartesian(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST((0.0, 0.0), 'Point'));

SELECT 'geoCoversCartesian: boundary point not contained (shows difference from geoContainsCartesian)';
SELECT geoContainsCartesian(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST((0.0, 0.0), 'Point'));

SELECT 'geoCoversCartesian: exterior point';
SELECT geoCoversCartesian(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST((5.0, 5.0), 'Point'));

SELECT 'geoCoversCartesian: point covers same point';
SELECT geoCoversCartesian(
    CAST((1.0, 2.0), 'Point'),
    CAST((1.0, 2.0), 'Point'));

SELECT 'geoCoversCartesian: point cannot cover polygon';
SELECT geoCoversCartesian(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoCoversCartesian: point in hole';
SELECT geoCoversCartesian(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)], [(0.003, 0.003), (0.007, 0.003), (0.007, 0.007), (0.003, 0.007), (0.003, 0.003)]], 'Polygon'),
    CAST((0.005, 0.005), 'Point'));

SELECT 'geoCoversCartesian: big polygon covers small';
SELECT geoCoversCartesian(
    CAST([[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.1, 0.1), (0.2, 0.1), (0.2, 0.2), (0.1, 0.2), (0.1, 0.1)]], 'Polygon'));

SELECT 'geoCoversSpherical: interior point';
SELECT geoCoversSpherical(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST((0.005, 0.005), 'Point'));

SELECT 'geoCoversSpherical: boundary point covered';
SELECT geoCoversSpherical(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST((0.0, 0.0), 'Point'));

SELECT 'geoCoversSpherical: big polygon covers small';
SELECT geoCoversSpherical(
    CAST([[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)]], 'Polygon'),
    CAST([[(0.1, 0.1), (0.2, 0.1), (0.2, 0.2), (0.1, 0.2), (0.1, 0.1)]], 'Polygon'));

SELECT 'ST_Covers is alias for geoCoversCartesian';
SELECT
    ST_Covers(CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'), CAST((0.005, 0.005), 'Point'))
    = geoCoversCartesian(CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'), CAST((0.005, 0.005), 'Point'));
