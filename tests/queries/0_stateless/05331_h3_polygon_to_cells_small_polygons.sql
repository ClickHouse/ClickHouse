-- Tags: no-fasttest

-- Polygons much smaller than a cell, in every containment mode: near a pentagon, close to the latitude where
-- the walk stops starting near the polygon, and next to the antimeridian. The cells span several parents.

SELECT name, res, mode, h3PolygonToCellsWithContainment(ring, res, mode)
FROM
(
    SELECT 'issue_ring' AS name, arrayMap(i -> (-0.4 + 0.0001 * cos(i), 51.4 + 0.0001 * sin(i)), range(9)) AS ring
    UNION ALL SELECT 'square', [(2.35, 48.85), (2.352, 48.85), (2.352, 48.852), (2.35, 48.852)]
    UNION ALL SELECT 'pentagon_ring', arrayMap(i -> (10.536199075467682 + 0.0003 * cos(2 * pi() * i / 7), 64.70000012793486 + 0.00015 * sin(2 * pi() * i / 7)), range(7))
    UNION ALL SELECT 'lat_79_9', arrayMap(i -> (20.1 + 0.002 * cos(2 * pi() * i / 5), 79.9 + 0.0004 * sin(2 * pi() * i / 5)), range(5))
    UNION ALL SELECT 'antimeridian', [(179.9991, -16.5), (179.9999, -16.5), (179.9999, -16.4995), (179.9991, -16.4995)]
)
ARRAY JOIN [9, 10, 11] AS res
ARRAY JOIN [0, 1, 2, 3] AS mode
ORDER BY name, res, mode;

-- `h3PolygonToCells` is mode 0.
SELECT h3PolygonToCells(arrayMap(i -> (-0.4 + 0.0001 * cos(i), 51.4 + 0.0001 * sin(i)), range(9)), 13);
SELECT h3PolygonToCells([(2.35, 48.85), (2.352, 48.85), (2.352, 48.852), (2.35, 48.852)], 10);
