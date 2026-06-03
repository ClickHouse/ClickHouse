-- Tags: no-fasttest

-- A MultiPolygon row accumulates cells across all its polygons: for two disjoint polygons the
-- combined result length equals the sum of the individual polygon result lengths.
WITH
    [[(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)]] AS poly_a,
    [[(10.0, 10.0), (10.0, 11.0), (11.0, 11.0), (11.0, 10.0), (10.0, 10.0)]] AS poly_b
SELECT
    length(h3PolygonToCells(CAST([poly_a, poly_b], 'MultiPolygon'), 4))
    = length(h3PolygonToCells(CAST(poly_a, 'Polygon'), 4)) + length(h3PolygonToCells(CAST(poly_b, 'Polygon'), 4));

-- A large polygon at a fine resolution exceeds the maximum array size and is rejected.
SELECT h3PolygonToCells(CAST([[(-10.0, -10.0), (-10.0, 10.0), (10.0, 10.0), (10.0, -10.0), (-10.0, -10.0)]], 'Polygon'), 15); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- MultiLineString is not a polygon and must be rejected instead of silently returning an empty array.
SELECT h3PolygonToCells(CAST([[(1.0, 2.0), (3.0, 4.0)], [(5.0, 6.0), (7.0, 8.0)]], 'MultiLineString'), 7); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
