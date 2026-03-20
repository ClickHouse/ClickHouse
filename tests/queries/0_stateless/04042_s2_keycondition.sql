-- Tags: no-fasttest
-- Test that s2RectContains and s2CapContains use KeyCondition for index pruning.
-- Correctness check: count with pruning must equal count without pruning
-- (no false negatives).

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
INSERT INTO t_s2_idx
SELECT
    number,
    geoToS2(
        toFloat64(number % 360) - 180.0,
        toFloat64(intDiv(number, 360) % 180) - 90.0
    )
FROM numbers(64800); -- 360 * 180 = full grid

-- s2RectContains: pruning must not lose any rows
SELECT
    (SELECT count() FROM t_s2_idx
     WHERE s2RectContains(geoToS2(-5.0, -5.0), geoToS2(5.0, 5.0), s2_loc))
    =
    (SELECT count() FROM t_s2_idx
     WHERE s2RectContains(geoToS2(-5.0, -5.0), geoToS2(5.0, 5.0), s2_loc)
     SETTINGS allow_experimental_s2_keycondition = 0);

-- s2CapContains: pruning must not lose any rows
SELECT
    (SELECT count() FROM t_s2_idx
     WHERE s2CapContains(geoToS2(0.0, 0.0), 5.0, s2_loc))
    =
    (SELECT count() FROM t_s2_idx
     WHERE s2CapContains(geoToS2(0.0, 0.0), 5.0, s2_loc)
     SETTINGS allow_experimental_s2_keycondition = 0);

DROP TABLE t_s2_idx;
