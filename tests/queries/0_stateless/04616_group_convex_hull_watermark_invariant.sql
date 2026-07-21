-- `groupConvexHull` compresses as soon as the number of points accumulated after the previous
-- compression exceeds 10,000. A writer-produced state exactly at the boundary must round-trip,
-- while a crafted watermark that lags by 10,001 points must be rejected before reading its point
-- payload.

SELECT 'watermark_growth_at_threshold';
SELECT serialized = hex(CAST(unhex(serialized) AS AggregateFunction(groupConvexHull, Point)))
FROM
(
    -- The first 10,001 identical points trigger compression; the remaining 10,000 stay as growth.
    SELECT hex(groupConvexHullState((0., 0.)::Point)) AS serialized
    FROM numbers(20001)
);

SELECT 'watermark_growth_above_threshold';
SELECT groupConvexHullMerge(state)
FROM
(
    SELECT CAST(unhex(concat(
        '02',    -- version
        '914E',  -- 10,001 points
        '00'     -- compression watermark = 0
    )) AS AggregateFunction(groupConvexHull, Point)) AS state
); -- { serverError INCORRECT_DATA }
