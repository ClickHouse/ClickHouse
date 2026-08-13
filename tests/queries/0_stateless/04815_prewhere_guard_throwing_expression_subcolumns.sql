-- Regression test: a PREWHERE condition that may throw must not be evaluated on rows rejected by a
-- preceding guard condition. All conditions of one PREWHERE read step are evaluated on the same
-- unfiltered block, so a guard and a throwing condition must not be placed into the same step.

-- With prewhere splitting disabled the whole PREWHERE is a single step, so the guard cannot protect
-- anything; that is a separate hazard of the WHERE -> PREWHERE move. Pin the setting, it is randomized in CI.
SET enable_multiple_prewhere_read_steps = 1;

DROP TABLE IF EXISTS t_prewhere_guard_json;

CREATE TABLE t_prewhere_guard_json (id UInt64, session_id UInt64, payload JSON, filler String)
ENGINE = MergeTree ORDER BY id;

-- One third of the rows have no coordinates, so the cast to Point throws unless the guard runs first.
-- The wide `filler` column makes the optimizer move the conditions to PREWHERE.
INSERT INTO t_prewhere_guard_json
SELECT
    number,
    number,
    if(number % 3 = 0, '{}', '{"longitude":4.35,"latitude":52.06}')::JSON,
    repeat('x', 200)
FROM numbers(100000);

SELECT '-- guard on payload.longitude protects the cast over payload.latitude';
SELECT count(DISTINCT session_id)
FROM
(
    SELECT
        session_id,
        CAST(payload.latitude AS Nullable(Float64)) AS latitude,
        CAST(payload.longitude AS Nullable(Float64)) AS longitude
    FROM t_prewhere_guard_json
    WHERE payload.longitude IS NOT NULL
) a
WHERE 1 = 1 -- the constant-true term leaves a residual filter in addition to the PREWHERE
    AND pointInPolygon((longitude, latitude)::Point, readWKTPolygon('POLYGON ((4.3 52.0,4.4 52.0,4.4 52.1,4.3 52.1,4.3 52.0))')) = 1;

SELECT '-- the same with both coordinates guarded';
SELECT count(DISTINCT session_id)
FROM
(
    SELECT
        session_id,
        CAST(payload.latitude AS Nullable(Float64)) AS latitude,
        CAST(payload.longitude AS Nullable(Float64)) AS longitude
    FROM t_prewhere_guard_json
    WHERE payload.longitude IS NOT NULL AND payload.latitude IS NOT NULL
) a
WHERE 1 = 1
    AND pointInPolygon((longitude, latitude)::Point, readWKTPolygon('POLYGON ((4.3 52.0,4.4 52.0,4.4 52.1,4.3 52.1,4.3 52.0))')) = 1;

DROP TABLE t_prewhere_guard_json;
