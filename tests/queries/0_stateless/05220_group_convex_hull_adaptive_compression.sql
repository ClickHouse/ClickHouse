SET max_threads = 1;
-- `groupConvexHull` is order-independent, so `RemoveRedundantSorting` would drop the subquery `ORDER BY`
-- that this test uses to vary the input order.
SET query_plan_remove_redundant_sorting = 0;

CREATE TEMPORARY TABLE hull_adaptive_points (id UInt64, pt Point);
INSERT INTO hull_adaptive_points
SELECT number, (cos(number * 2 * pi() / 35004), sin(number * 2 * pi() / 35004))
FROM numbers(35004);

-- All points are vertices. Adaptive accumulation and compact serialization must
-- preserve every vertex across a binary round trip.
CREATE TEMPORARY TABLE hull_adaptive_state (s AggregateFunction(groupConvexHull, Point));
INSERT INTO hull_adaptive_state SELECT groupConvexHullState(pt) FROM hull_adaptive_points;

SELECT 'adaptive_state_roundtrip';
SELECT
    startsWith(hex(s), '03'),
    hex(s) = hex(CAST(unhex(hex(s)) AS AggregateFunction(groupConvexHull, Point))),
    length(finalizeAggregation(s)) = 35005,
    abs(polygonAreaCartesian(finalizeAggregation(s)) - 35004 * sin(2 * pi() / 35004) / 2) < 1e-10
FROM hull_adaptive_state;

-- Merging a restored state into an empty accumulator reproduces it byte for byte.
SELECT 'adaptive_first_merge';
SELECT any(hex(s)) = hex(groupConvexHullMergeState(
    CAST(unhex(hex(s)) AS AggregateFunction(groupConvexHull, Point))))
FROM hull_adaptive_state;

-- Partial states merged in descending order produce the full hull.
SELECT 'adaptive_partial_merge';
SELECT length(groupConvexHullMerge(s)) = 35005
FROM
(
    SELECT intDiv(id, 17502) AS part,
        CAST(unhex(hex(groupConvexHullState(pt))) AS AggregateFunction(groupConvexHull, Point)) AS s
    FROM hull_adaptive_points
    GROUP BY part
    ORDER BY part DESC
);

-- Only the current state version is accepted.
SELECT 'other_version_rejected';
SELECT finalizeAggregation(CAST(unhex(concat('02', substring(hex(s), 3)))
    AS AggregateFunction(groupConvexHull, Point)))
FROM hull_adaptive_state; -- { serverError INCORRECT_DATA }

-- The format allows an uncompressed suffix proportional to the compressed prefix. The writer
-- compacts it, so construct such metadata explicitly: 35,004 points, watermark 20,003.
SELECT 'adaptive_suffix';
SELECT length(finalizeAggregation(CAST(unhex(concat('03BC9102A39C01', substring(hex(s), 15)))
    AS AggregateFunction(groupConvexHull, Point)))) = 35005 FROM hull_adaptive_state;

-- Version 3 still rejects a growth gap beyond its adaptive threshold before reading payload.
SELECT 'adaptive_rejects_excess_growth';
SELECT finalizeAggregation(CAST(unhex('03C7B802A39C01')
    AS AggregateFunction(groupConvexHull, Point))); -- { serverError INCORRECT_DATA }

SELECT 'adaptive_rejects_invalid_watermark';
SELECT finalizeAggregation(CAST(unhex('030102')
    AS AggregateFunction(groupConvexHull, Point))); -- { serverError INCORRECT_DATA }

SELECT 'adaptive_rejects_nonfinite';
SELECT finalizeAggregation(CAST(unhex('030100000000000000F07F0000000000000000')
    AS AggregateFunction(groupConvexHull, Point))); -- { serverError INCORRECT_DATA }

-- A serialized watermark is not proof that its prefix is a convex hull. Four points below
-- include one interior point; finalization must still compute the three-vertex hull.
SELECT 'untrusted_compressed_prefix';
SELECT length(finalizeAggregation(CAST(unhex(concat(
    '030404',
    '00000000000000000000000000000000',
    '000000000000F03F0000000000000000',
    '0000000000000000000000000000F03F',
    '000000000000D03F000000000000D03F')) AS AggregateFunction(groupConvexHull, Point)))) = 4;
