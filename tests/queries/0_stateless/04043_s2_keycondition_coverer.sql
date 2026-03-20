-- Tags: no-fasttest
-- Test S2RegionCoverer-based index pruning for s2RectContains and s2CapContains

SET allow_experimental_s2_keycondition = 1;
SET s2_keycondition_use_coverer = 1;

DROP TABLE IF EXISTS t_s2_coverer;
CREATE TABLE t_s2_coverer (id UInt64, s2_loc UInt64)
ENGINE = MergeTree ORDER BY s2_loc SETTINGS index_granularity = 100;

INSERT INTO t_s2_coverer
SELECT number, geoToS2(toFloat64(number % 360) - 180.0, toFloat64(intDiv(number, 360) % 180) - 90.0)
FROM numbers(64800);

-- Coverer mode must return same row counts as ancestor mode (no false negatives)
SELECT
    (SELECT count() FROM t_s2_coverer
     WHERE s2RectContains(geoToS2(-5.0, -5.0), geoToS2(5.0, 5.0), s2_loc))
    =
    (SELECT count() FROM t_s2_coverer
     WHERE s2RectContains(geoToS2(-5.0, -5.0), geoToS2(5.0, 5.0), s2_loc)
     SETTINGS s2_keycondition_use_coverer = 0);

SELECT
    (SELECT count() FROM t_s2_coverer
     WHERE s2CapContains(geoToS2(0.0, 0.0), 5.0, s2_loc))
    =
    (SELECT count() FROM t_s2_coverer
     WHERE s2CapContains(geoToS2(0.0, 0.0), 5.0, s2_loc)
     SETTINGS s2_keycondition_use_coverer = 0);

-- EXPLAIN still shows function name as key condition
SELECT replaceRegexpAll(explain, '.*Condition: (.*)', '\\1') AS cond
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_s2_coverer
      WHERE s2RectContains(geoToS2(-10.0, -10.0), geoToS2(10.0, 10.0), s2_loc))
WHERE explain LIKE '%Condition:%' FORMAT TSVRaw;

SELECT replaceRegexpAll(explain, '.*Condition: (.*)', '\\1') AS cond
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_s2_coverer
      WHERE s2CapContains(geoToS2(0.0, 0.0), 10.0, s2_loc))
WHERE explain LIKE '%Condition:%' FORMAT TSVRaw;

DROP TABLE t_s2_coverer;
