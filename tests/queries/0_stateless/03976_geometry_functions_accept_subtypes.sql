-- Test that geometry functions (perimeterSpherical, areaSpherical, etc.) accept
-- individual geometry subtypes (Polygon, Ring, etc.) in addition to the Geometry variant type.
-- https://github.com/ClickHouse/ClickHouse/issues/95880

SELECT 'perimeterSpherical with readWKTPolygon';
SELECT round(perimeterSpherical(readWKTPolygon('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')), 6);

SELECT 'areaSpherical with readWKTPolygon';
SELECT round(areaSpherical(readWKTPolygon('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')), 6);

SELECT 'perimeterCartesian with readWKTPolygon';
SELECT perimeterCartesian(readWKTPolygon('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));

SELECT 'areaCartesian with readWKTPolygon';
SELECT areaCartesian(readWKTPolygon('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));

SELECT 'perimeterSpherical with readWKTRing';
SELECT round(perimeterSpherical(readWKTRing('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')), 6);

SELECT 'perimeterSpherical with readWKTPoint';
SELECT round(perimeterSpherical(readWKTPoint('POINT(0 0)')), 6);

SELECT 'perimeterSpherical with readWKTMultiPolygon';
SELECT round(perimeterSpherical(readWKTMultiPolygon('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)))')), 6);

SELECT 'perimeterSpherical with readWKT (Geometry variant)';
SELECT round(perimeterSpherical(readWKT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')), 6);

SELECT 'perimeterSpherical with column alias';
WITH 'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))' AS wkt SELECT readWKTPolygon(wkt) AS w2, round(perimeterSpherical(w2), 6);
