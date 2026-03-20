-- Tags: no-fasttest
-- Test range-intersect mode: covering tested against granule's actual
-- [cell_min, cell_max] interval instead of the common ancestor.

SET allow_experimental_s2_keycondition = 1;
SET s2_keycondition_use_coverer = 1;
SET s2_keycondition_use_range_intersect = 1;

DROP TABLE IF EXISTS t_s2_range;
CREATE TABLE t_s2_range (id UInt64, s2_loc UInt64)
ENGINE = MergeTree ORDER BY s2_loc SETTINGS index_granularity = 100;

INSERT INTO t_s2_range
SELECT number, geoToS2(toFloat64(number % 360) - 180.0, toFloat64(intDiv(number, 360) % 180) - 90.0)
FROM numbers(64800);

-- Range-intersect mode must return same row counts as coverer mode (no false negatives).
SELECT
    (SELECT count() FROM t_s2_range
     WHERE s2RectContains(geoToS2(-5.0, -5.0), geoToS2(5.0, 5.0), s2_loc))
    =
    (SELECT count() FROM t_s2_range
     WHERE s2RectContains(geoToS2(-5.0, -5.0), geoToS2(5.0, 5.0), s2_loc)
     SETTINGS s2_keycondition_use_range_intersect = 0);

SELECT
    (SELECT count() FROM t_s2_range
     WHERE s2CapContains(geoToS2(0.0, 0.0), 5.0, s2_loc))
    =
    (SELECT count() FROM t_s2_range
     WHERE s2CapContains(geoToS2(0.0, 0.0), 5.0, s2_loc)
     SETTINGS s2_keycondition_use_range_intersect = 0);

DROP TABLE t_s2_range;
