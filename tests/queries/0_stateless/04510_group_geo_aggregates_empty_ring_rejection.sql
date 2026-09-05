-- A valid polygonal aggregate state never serializes a ring with fewer than four points or an empty
-- `MultiPolygon` chunk: the add path skips empty polygons and rejects a polygon with an empty outer
-- but non-empty inner rings, and boost union/intersection output is OGC-valid. Empty structures
-- and short rings only ever appear in a crafted state. `deserializeGeoRing` rejects them on sight,
-- so every allocated ring carries at least four points and is charged against the point budget;
-- `deserializeGeoMultiPolygon` rejects a zero-polygon chunk before it enters aggregate state.

-- 1. `groupPolygonUnion`: a complete polygon with an empty outer ring. This isolates the eager ring
--    guard without relying on a second malformed field.
SELECT 'union_empty_outer_ring';
SELECT groupPolygonUnionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',          -- version
        '01',          -- 1 chunk
        '01',          -- 1 polygon
        '00',          -- 0-point outer ring
        '00'           -- 0 inner rings
    )) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 2. `groupPolygonUnion`: a valid outer ring but an empty inner ring (hole). The add
--    path never produces an empty hole, so this is rejected while the hole is read.
SELECT 'union_empty_inner_ring';
SELECT groupPolygonUnionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',                  -- version
        '01',                  -- 1 chunk
        '01',                  -- 1 polygon
        '04',                  -- 4-point closed outer ring
        '0000000000000000', '0000000000000000', -- (0, 0)
        '000000000000F03F', '0000000000000000', -- (1, 0)
        '0000000000000000', '000000000000F03F', -- (0, 1)
        '0000000000000000', '0000000000000000', -- (0, 0)
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
        '04',                  -- polygon 1: 4-point closed outer ring
        '0000000000000000', '0000000000000000', -- (0, 0)
        '000000000000F03F', '0000000000000000', -- (1, 0)
        '0000000000000000', '000000000000F03F', -- (0, 1)
        '0000000000000000', '0000000000000000', -- (0, 0)
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

-- 5. `groupPolygonIntersection`: an empty inner ring (hole) is rejected on the same path.
SELECT 'intersect_empty_inner_ring';
SELECT groupPolygonIntersectionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',                  -- version
        '01',                  -- mode = NonEmpty
        '01',                  -- 1 chunk
        '01',                  -- 1 polygon
        '04',                  -- 4-point closed outer ring
        '0000000000000000', '0000000000000000', -- (0, 0)
        '000000000000F03F', '0000000000000000', -- (1, 0)
        '0000000000000000', '000000000000F03F', -- (0, 1)
        '0000000000000000', '0000000000000000', -- (0, 0)
        '01',                  -- 1 inner ring
        '00'                   -- 0-point inner ring (empty hole)
    )) AS AggregateFunction(groupPolygonIntersection, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 6. `groupPolygonUnion`: a chunk with no polygons cannot be produced by the writer.
SELECT 'union_empty_multipolygon_chunk';
SELECT groupPolygonUnionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',  -- version
        '01',  -- 1 chunk
        '00'   -- 0 polygons
    )) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 7. `groupPolygonIntersection`: NonEmpty mode with an empty chunk cannot be produced by the writer.
SELECT 'intersect_empty_multipolygon_chunk';
SELECT groupPolygonIntersectionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',  -- version
        '01',  -- mode = NonEmpty
        '01',  -- 1 chunk
        '00'   -- 0 polygons
    )) AS AggregateFunction(groupPolygonIntersection, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 8. Positive round-trip: a valid polygon with a real (non-empty) hole must still round-trip. The
--    eager short-ring guard must not reject legitimate inner rings. The result keeps the outer
--    ring plus the one hole (2 rings).
SELECT 'union_polygon_with_hole_roundtrip';
SELECT length(arrayElement(groupPolygonUnionMerge(state), 1)) FROM (
    SELECT CAST(unhex(hex(groupPolygonUnionState(poly))) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state FROM (
        SELECT [
            [(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)],
            [(3., 3.), (3., 7.), (7., 7.), (7., 3.), (3., 3.)]
        ]::Polygon AS poly
    )
);

-- 9. A three-point, unclosed triangle is repairable by `boost::geometry::correct`, but the writer
--    only serializes the corrected four-point ring. The reader must reject this unreachable shape
--    before normalization; otherwise it would silently accept a state the writer cannot emit.
SELECT 'union_short_repairable_ring';
SELECT groupPolygonUnionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',                  -- version
        '01',                  -- 1 chunk
        '01',                  -- 1 polygon
        '03',                  -- 3-point unclosed triangle
        '0000000000000000', '0000000000000000', -- (0, 0)
        '000000000000F03F', '0000000000000000', -- (1, 0)
        '0000000000000000', '000000000000F03F', -- (0, 1)
        '00'                   -- 0 inner rings
    )) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

SELECT 'intersect_short_repairable_ring';
SELECT groupPolygonIntersectionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',                  -- version
        '01',                  -- mode = NonEmpty
        '01',                  -- 1 chunk
        '01',                  -- 1 polygon
        '03',                  -- 3-point unclosed triangle
        '0000000000000000', '0000000000000000', -- (0, 0)
        '000000000000F03F', '0000000000000000', -- (1, 0)
        '0000000000000000', '000000000000F03F', -- (0, 1)
        '00'                   -- 0 inner rings
    )) AS AggregateFunction(groupPolygonIntersection, Polygon)) AS state
); -- { serverError INCORRECT_DATA }
