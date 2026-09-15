SET max_threads = 1;

CREATE TEMPORARY TABLE hull_adaptive_points (id UInt64, pt Point);
INSERT INTO hull_adaptive_points
SELECT number, (cos(number * 2 * pi() / 35004), sin(number * 2 * pi() / 35004))
FROM numbers(35004);

-- All points are vertices. The state contains more than 10,000 fresh points after
-- its last compression, so a version-2 reader would incorrectly reject this writer's output.
CREATE TEMPORARY TABLE hull_adaptive_state (s AggregateFunction(groupConvexHull, Point));
INSERT INTO hull_adaptive_state SELECT groupConvexHullState(pt) FROM hull_adaptive_points;

SELECT 'adaptive_state_roundtrip';
SELECT
    startsWith(hex(s), '03'),
    hex(s) = hex(CAST(unhex(hex(s)) AS AggregateFunction(groupConvexHull, Point))),
    length(finalizeAggregation(s)) = 35005,
    abs(polygonAreaCartesian(finalizeAggregation(s)) - 35004 * sin(2 * pi() / 35004) / 2) < 1e-10
FROM hull_adaptive_state;

-- Copying a restored state into an empty accumulator must preserve the adaptive watermark.
SELECT 'adaptive_first_merge';
SELECT any(hex(s)) = hex(groupConvexHullMergeState(
    CAST(unhex(hex(s)) AS AggregateFunction(groupConvexHull, Point))))
FROM hull_adaptive_state;

-- Partial states and both merge orders produce the same mathematical hull.
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

-- A genuine small legacy state has the same payload fields. The new reader accepts version 2
-- and reserializes it as version 3 without losing points or changing the result.
SELECT 'legacy_state';
SELECT
    length(groupConvexHullMerge(s)) = 5,
    startsWith(hex(groupConvexHullMergeState(s)), '03')
FROM
(
    SELECT CAST(unhex(concat('02', substring(hex(groupConvexHullState(pt)), 3)))
        AS AggregateFunction(groupConvexHull, Point)) AS s
    FROM
    (
        SELECT (toFloat64(number % 2), toFloat64(intDiv(number, 2) % 2))::Point AS pt
        FROM numbers(10000)
    )
);

-- Downgrading an adaptive state with >10,000 fresh points must fail the legacy invariant.
SELECT 'legacy_rejects_adaptive_gap';
SELECT finalizeAggregation(CAST(unhex(concat('02', substring(hex(s), 3)))
    AS AggregateFunction(groupConvexHull, Point)))
FROM hull_adaptive_state; -- { serverError INCORRECT_DATA }

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
