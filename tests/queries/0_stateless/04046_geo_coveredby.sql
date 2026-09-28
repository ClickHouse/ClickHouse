-- Tags: no-fasttest

SELECT 'geoCoveredByCartesian: interior point covered';
SELECT geoCoveredByCartesian(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoCoveredByCartesian: boundary point covered';
SELECT geoCoveredByCartesian(
    CAST((0.0, 0.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoCoveredByCartesian: boundary point not within (shows difference)';
SELECT geoWithinCartesian(
    CAST((0.0, 0.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoCoveredByCartesian: exterior point';
SELECT geoCoveredByCartesian(
    CAST((5.0, 5.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoCoveredByCartesian: polygon not covered by point';
SELECT geoCoveredByCartesian(
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'),
    CAST((0.005, 0.005), 'Point'));

SELECT 'geoCoveredByCartesian: point in hole';
SELECT geoCoveredByCartesian(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)], [(0.003, 0.003), (0.007, 0.003), (0.007, 0.007), (0.003, 0.007), (0.003, 0.003)]], 'Polygon'));

SELECT 'geoCoveredByCartesian: const polygon, variable points';
SELECT geoCoveredByCartesian(
    CAST(p, 'Point'),
    CAST([[(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]], 'Polygon'))
FROM VALUES('p Tuple(Float64, Float64)',
    (5.0, 5.0), (15.0, 5.0));

SELECT 'geoCoveredBySpherical: interior point covered';
SELECT geoCoveredBySpherical(
    CAST((0.005, 0.005), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoCoveredBySpherical: boundary point covered';
SELECT geoCoveredBySpherical(
    CAST((0.0, 0.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'geoCoveredBySpherical: exterior point';
SELECT geoCoveredBySpherical(
    CAST((5.0, 5.0), 'Point'),
    CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'ST_CoveredBy is alias for geoCoveredByCartesian';
SELECT
    ST_CoveredBy(CAST((0.005, 0.005), 'Point'), CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'))
    = geoCoveredByCartesian(CAST((0.005, 0.005), 'Point'), CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'));

SELECT 'symmetry: geoCoveredByCartesian(A,B) == geoCoversCartesian(B,A)';
SELECT
    geoCoveredByCartesian(CAST((0.005, 0.005), 'Point'), CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'))
    = geoCoversCartesian(CAST([[(0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)]], 'Polygon'), CAST((0.005, 0.005), 'Point'));
