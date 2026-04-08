-- Malformed binary state rejection for geo aggregate functions.

-- 1. groupConvexHull: reject unknown version (2 instead of 1)
SELECT 'convex_hull_bad_version';
SELECT groupConvexHullMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '02',
        substring(hex(groupConvexHullState(pt)), 3)
    )) AS AggregateFunction(groupConvexHull, Point)) AS state
    FROM (SELECT readWKTPoint('POINT (1 2)') AS pt)
); -- { serverError INCORRECT_DATA }

-- 2. groupPolygonUnion: reject oversized chunk count (varint 100000)
SELECT 'union_oversized_chunks';
SELECT groupPolygonUnionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',      -- version
        'A08D06',  -- 100000 chunks (limit 10000)
        '00'
    )) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 3. groupPolygonIntersection: reject invalid mode byte (3, valid: 0-2)
SELECT 'intersect_bad_mode';
SELECT groupPolygonIntersectionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',  -- version
        '03'   -- invalid mode
    )) AS AggregateFunction(groupPolygonIntersection, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 4. groupPolygonIntersection: reject NonEmpty mode with zero chunks
SELECT 'intersect_nonempty_zero_chunks';
SELECT groupPolygonIntersectionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',  -- version
        '01',  -- mode = NonEmpty
        '00'   -- 0 chunks
    )) AS AggregateFunction(groupPolygonIntersection, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 5. groupPolygonUnion: reject oversized polygon count (varint 100000)
SELECT 'union_oversized_polygons';
SELECT groupPolygonUnionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',      -- version
        '01',      -- 1 chunk
        'A08D06',  -- 100000 polygons (limit 10000)
        '00'
    )) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 6. groupPolygonUnion: reject oversized inner ring count (varint 100000)
SELECT 'union_oversized_rings';
SELECT groupPolygonUnionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',      -- version
        '01',      -- 1 chunk
        '01',      -- 1 polygon
        '00',      -- 0-point outer ring
        'A08D06',  -- 100000 inner rings (limit 10000)
        '00'
    )) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 7. groupPolygonUnion: reject oversized point count per ring (varint 10000001)
SELECT 'union_oversized_points';
SELECT groupPolygonUnionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',          -- version
        '01',          -- 1 chunk
        '01',          -- 1 polygon
        '81ADE204',    -- 10000001 points (limit 10000000)
        '00'
    )) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 8. groupConvexHull: reject oversized point count (varint 100000001)
SELECT 'convex_hull_oversized_points';
SELECT groupConvexHullMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',          -- version
        '81C2D72F',    -- 100000001 points (limit 100000000)
        '00'
    )) AS AggregateFunction(groupConvexHull, Point)) AS state
); -- { serverError INCORRECT_DATA }

-- 9. groupPolygonUnion: reject NaN coordinate in deserialized state
SELECT 'union_nan_coordinate';
SELECT groupPolygonUnionMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',                  -- version
        '01',                  -- 1 chunk
        '01',                  -- 1 polygon
        '01',                  -- 1-point outer ring
        '000000000000F87F',    -- x = NaN (little-endian IEEE 754)
        '0000000000000000',    -- y = 0.0
        '00'                   -- 0 inner rings
    )) AS AggregateFunction(groupPolygonUnion, Polygon)) AS state
); -- { serverError INCORRECT_DATA }

-- 10. groupConvexHull: reject +Inf coordinate in deserialized state
SELECT 'convex_hull_inf_coordinate';
SELECT groupConvexHullMerge(state) FROM (
    SELECT CAST(unhex(concat(
        '01',                  -- version
        '01',                  -- 1 point
        '000000000000F07F',    -- x = +Inf (little-endian IEEE 754)
        '0000000000000000'     -- y = 0.0
    )) AS AggregateFunction(groupConvexHull, Point)) AS state
); -- { serverError INCORRECT_DATA }
