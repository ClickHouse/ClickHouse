-- Tags: no-fasttest
-- Test that s2RectContains and s2CapContains use KeyCondition for index pruning

SET allow_experimental_s2_keycondition = 1;

DROP TABLE IF EXISTS t_s2_idx;
CREATE TABLE t_s2_idx
(
    id UInt64,
    s2_loc UInt64
)
ENGINE = MergeTree
ORDER BY s2_loc
SETTINGS index_granularity = 100;

-- Insert S2 cells covering a grid of locations.
-- Use independent lon/lat to get good coverage.
INSERT INTO t_s2_idx
SELECT
    number,
    geoToS2(
        toFloat64(number % 360) - 180.0,
        toFloat64(intDiv(number, 360) % 180) - 90.0
    )
FROM numbers(64800); -- 360 * 180 = full grid

-- Verify s2RectContains returns results
SELECT count() > 0 FROM t_s2_idx
WHERE s2RectContains(geoToS2(-10.0, -10.0), geoToS2(10.0, 10.0), s2_loc);

-- Verify s2CapContains returns results
SELECT count() > 0 FROM t_s2_idx
WHERE s2CapContains(geoToS2(0.0, 0.0), 10.0, s2_loc);

-- Verify KeyCondition recognizes s2RectContains (condition must contain function name, not 'unknown')
SELECT replaceRegexpAll(explain, '.*Condition: (.*)', '\\1') AS cond
FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM t_s2_idx
    WHERE s2RectContains(geoToS2(-10.0, -10.0), geoToS2(10.0, 10.0), s2_loc)
)
WHERE explain LIKE '%Condition:%'
FORMAT TSVRaw
;

-- Verify KeyCondition recognizes s2CapContains
SELECT replaceRegexpAll(explain, '.*Condition: (.*)', '\\1') AS cond
FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM t_s2_idx
    WHERE s2CapContains(geoToS2(0.0, 0.0), 10.0, s2_loc)
)
WHERE explain LIKE '%Condition:%'
FORMAT TSVRaw
;

DROP TABLE t_s2_idx;
