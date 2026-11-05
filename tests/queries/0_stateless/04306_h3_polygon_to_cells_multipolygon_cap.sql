-- Tags: no-fasttest

-- A MultiPolygon row accumulates cells across all its polygons: for two disjoint polygons the
-- combined result length equals the sum of the individual polygon result lengths.
WITH
    [[(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)]] AS poly_a,
    [[(10.0, 10.0), (10.0, 11.0), (11.0, 11.0), (11.0, 10.0), (10.0, 10.0)]] AS poly_b
SELECT
    length(h3PolygonToCells(CAST([poly_a, poly_b], 'MultiPolygon'), 4))
    = length(h3PolygonToCells(CAST(poly_a, 'Polygon'), 4)) + length(h3PolygonToCells(CAST(poly_b, 'Polygon'), 4));

-- An empty MultiPolygon after a non-empty row must produce valid monotonic offsets (second row empty array).
WITH
    CAST([[[(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0)]]], 'MultiPolygon') AS nonempty_mp,
    CAST([], 'MultiPolygon') AS empty_mp
SELECT countIf(cnt > 0) = 1 AND countIf(cnt = 0) = 1 AND count() = 2
FROM
(
    SELECT length(h3PolygonToCells(mp, 4)) AS cnt
    FROM (SELECT arrayJoin([nonempty_mp, empty_mp]) AS mp)
);

-- The per-row cumulative cap requires several polygons whose sizes sum past MAX_ARRAY_SIZE; each such
-- polygon alone needs multiple gigabytes, so it cannot be triggered cheaply in a stateless test.
-- A single large polygon at a fine resolution exceeds the limit in the per-polygon size estimate (before
-- any allocation) and is rejected, which guards against regressions in the size cap after the refactor.
SELECT h3PolygonToCells(CAST([[(-10.0, -10.0), (-10.0, 10.0), (10.0, 10.0), (10.0, -10.0), (-10.0, -10.0)]], 'Polygon'), 15); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- MultiLineString is not a polygon and must be rejected instead of silently returning an empty array.
SELECT h3PolygonToCells(CAST([[(1.0, 2.0), (3.0, 4.0)], [(5.0, 6.0), (7.0, 8.0)]], 'MultiLineString'), 7); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
