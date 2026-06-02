-- Deserialization hardening for polygonal aggregate states.
-- Tests the cumulative point budget and post-deserialization validity check.

-- 1. Cumulative point budget: two polygons in one multipolygon where
--    each polygon's ring passes the per-ring cap (10M) but the total
--    exceeds MAX_POINTS_IN_POLYGONAL_STATE (10M).
--    Polygon 1: 1 real point.  Polygon 2 claims 10,000,000 points.
--    Per-ring: 10,000,000 <= 10,000,000.  Cumulative: 10,000,001 > 10,000,000.
SELECT 'union_cumulative_budget';
SELECT groupPolygonUnionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',                  -- version
        '01',                  -- 1 chunk
        '02',                  -- 2 polygons
        '01',                  -- polygon 1: 1-point outer ring
        '000000000000F03F',    -- x = 1.0
        '0000000000000040',    -- y = 2.0
        '00',                  -- 0 inner rings
        '80ADE204'             -- polygon 2: 10,000,000-point outer ring (budget exceeded)
    )) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 2. Same check for groupPolygonIntersection.
SELECT 'intersect_cumulative_budget';
SELECT groupPolygonIntersectionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',                  -- version
        '01',                  -- mode = NonEmpty
        '01',                  -- 1 chunk
        '02',                  -- 2 polygons
        '01',                  -- polygon 1: 1-point outer ring
        '000000000000F03F',    -- x = 1.0
        '0000000000000040',    -- y = 2.0
        '00',                  -- 0 inner rings
        '80ADE204'             -- polygon 2: 10,000,000-point outer ring (budget exceeded)
    )) AS AggregateFunction(groupPolygonIntersection, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 3. Self-intersecting (bowtie) polygon: finite coordinates but invalid topology.
--    Outer ring: (0,0) -> (2,2) -> (2,0) -> (0,2) -> (0,0) — a figure-8.
--    The add path rejects this via boost::geometry::is_valid; deserialization
--    must now apply the same invariant.
SELECT 'union_invalid_topology';
SELECT groupPolygonUnionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',                  -- version
        '01',                  -- 1 chunk
        '01',                  -- 1 polygon
        '05',                  -- 5-point outer ring
        '0000000000000000', '0000000000000000',    -- (0, 0)
        '0000000000000040', '0000000000000040',    -- (2, 2)
        '0000000000000040', '0000000000000000',    -- (2, 0)
        '0000000000000000', '0000000000000040',    -- (0, 2)
        '0000000000000000', '0000000000000000',    -- (0, 0)
        '00'                   -- 0 inner rings
    )) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 4. Same invalid-topology check for groupPolygonIntersection.
SELECT 'intersect_invalid_topology';
SELECT groupPolygonIntersectionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',                  -- version
        '01',                  -- mode = NonEmpty
        '01',                  -- 1 chunk
        '01',                  -- 1 polygon
        '05',                  -- 5-point outer ring
        '0000000000000000', '0000000000000000',    -- (0, 0)
        '0000000000000040', '0000000000000040',    -- (2, 2)
        '0000000000000040', '0000000000000000',    -- (2, 0)
        '0000000000000000', '0000000000000040',    -- (0, 2)
        '0000000000000000', '0000000000000000',    -- (0, 0)
        '00'                   -- 0 inner rings
    )) AS AggregateFunction(groupPolygonIntersection, Polygon)) AS state
); -- { serverError INCORRECT_DATA }
