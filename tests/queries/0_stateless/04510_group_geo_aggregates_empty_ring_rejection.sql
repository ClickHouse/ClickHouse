-- A valid polygonal aggregate state never serializes an empty (zero-point) ring: the add path
-- skips empty polygons and rejects a polygon with an empty outer but non-empty inner rings, and
-- boost union/intersection output is OGC-valid. Empty rings only ever appear in a crafted state,
-- where they are pure metadata carrying no points. Without an eager guard, a payload advertising a
-- large polygon or inner-ring count with every ring of size 0 stays within the cumulative ring
-- budget yet materializes millions of empty rings before validation runs, amplifying a tiny
-- payload into a huge allocation. deserializeGeoRing now rejects a zero-point ring on sight, so
-- every allocated ring must carry at least one point and is charged against the point budget.

-- 1. groupPolygonUnion: the exact reported amplification shape — one polygon with a zero-point
--    outer ring, followed by a huge inner-ring count. Rejected eagerly at the empty outer ring,
--    before the inner-ring count is even read.
SELECT 'union_empty_outer_ring';
SELECT groupPolygonUnionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',          -- version
        '01',          -- 1 chunk
        '01',          -- 1 polygon
        '00',          -- 0-point outer ring
        '80ADE204'     -- 10000000 inner rings (never reached)
    )) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 2. groupPolygonUnion: a valid single-point outer ring but an empty inner ring (hole). The add
--    path never produces an empty hole, so this is rejected while the hole is read.
SELECT 'union_empty_inner_ring';
SELECT groupPolygonUnionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',                  -- version
        '01',                  -- 1 chunk
        '01',                  -- 1 polygon
        '01',                  -- 1-point outer ring
        '0000000000000000',    -- x = 0.0
        '0000000000000000',    -- y = 0.0
        '01',                  -- 1 inner ring
        '00'                   -- 0-point inner ring (empty hole)
    )) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 3. groupPolygonUnion: the multipolygon polygon-count amplification. A first non-empty polygon,
--    then a second polygon with an empty outer ring. Rejected at the second polygon's outer ring,
--    so a large polygon count cannot materialize empty polygon metadata.
SELECT 'union_empty_outer_second_polygon';
SELECT groupPolygonUnionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',                  -- version
        '01',                  -- 1 chunk
        '02',                  -- 2 polygons
        '01',                  -- polygon 1: 1-point outer ring
        '0000000000000000',    -- x = 0.0
        '0000000000000000',    -- y = 0.0
        '00',                  -- polygon 1: 0 inner rings
        '00'                   -- polygon 2: 0-point outer ring
    )) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 4. groupPolygonIntersection: an empty outer ring is rejected on the same path.
SELECT 'intersect_empty_outer_ring';
SELECT groupPolygonIntersectionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',          -- version
        '01',          -- mode = NonEmpty
        '01',          -- 1 chunk
        '01',          -- 1 polygon
        '00'           -- 0-point outer ring
    )) AS AggregateFunction(groupPolygonIntersection, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 5. groupPolygonIntersection: an empty inner ring (hole) is rejected on the same path.
SELECT 'intersect_empty_inner_ring';
SELECT groupPolygonIntersectionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',                  -- version
        '01',                  -- mode = NonEmpty
        '01',                  -- 1 chunk
        '01',                  -- 1 polygon
        '01',                  -- 1-point outer ring
        '0000000000000000',    -- x = 0.0
        '0000000000000000',    -- y = 0.0
        '01',                  -- 1 inner ring
        '00'                   -- 0-point inner ring (empty hole)
    )) AS AggregateFunction(groupPolygonIntersection, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 6. Positive round-trip: a valid polygon with a real (non-empty) hole must still round-trip. The
--    eager zero-point-ring guard must not reject legitimate inner rings. The result keeps the outer
--    ring plus the one hole (2 rings).
SELECT 'union_polygon_with_hole_roundtrip';
SELECT length(arrayElement(groupPolygonUnionMerge(state), 1)) FROM (
    SELECT groupPolygonUnionState(poly) AS state FROM (
        SELECT [
            [(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)],
            [(3., 3.), (3., 7.), (7., 7.), (7., 3.), (3., 3.)]
        ]::Polygon AS poly
    )
);
