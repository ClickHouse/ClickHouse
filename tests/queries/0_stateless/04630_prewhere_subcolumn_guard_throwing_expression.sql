SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET enable_multiple_prewhere_read_steps = 1;
SET move_all_conditions_to_prewhere = 1;

-- A guard predicate and a potentially throwing predicate over subcolumns of the same physical
-- column must not share a PREWHERE read step, otherwise the throwing predicate is evaluated on
-- the rows the guard rejects.

DROP TABLE IF EXISTS t_prewhere_guard_json;
CREATE TABLE t_prewhere_guard_json (id Int64, session_id Int64, payload JSON, filler String)
ENGINE = MergeTree ORDER BY id;

-- 2/3 of the rows have coordinates, 1/3 are NULL and must be filtered by the guard.
INSERT INTO t_prewhere_guard_json
SELECT number, number,
       if(number % 3 = 0, '{}', '{"longitude":4.35,"latitude":52.06}')::JSON,
       repeat('x', 200)
FROM numbers(100000);

SELECT 'json guard, default settings';
SELECT count(DISTINCT session_id)
FROM (
    SELECT session_id,
           CAST(payload.latitude AS Nullable(Float64)) AS latitude,
           CAST(payload.longitude AS Nullable(Float64)) AS longitude
    FROM t_prewhere_guard_json
    WHERE payload.longitude IS NOT NULL
) a
WHERE 1 = 1
  AND pointInPolygon((longitude, latitude)::Point,
        readWKTPolygon('POLYGON ((4.3 52.0,4.4 52.0,4.4 52.1,4.3 52.1,4.3 52.0))')) = 1;

SELECT 'json guard, both coordinates guarded';
SELECT count(DISTINCT session_id)
FROM (
    SELECT session_id,
           CAST(payload.latitude AS Nullable(Float64)) AS latitude,
           CAST(payload.longitude AS Nullable(Float64)) AS longitude
    FROM t_prewhere_guard_json
    WHERE payload.longitude IS NOT NULL AND payload.latitude IS NOT NULL
) a
WHERE 1 = 1
  AND pointInPolygon((longitude, latitude)::Point,
        readWKTPolygon('POLYGON ((4.3 52.0,4.4 52.0,4.4 52.1,4.3 52.1,4.3 52.0))')) = 1;

SELECT 'json guard, allow_reorder_prewhere_conditions = 0';
SELECT count(DISTINCT session_id)
FROM (
    SELECT session_id,
           CAST(payload.latitude AS Nullable(Float64)) AS latitude,
           CAST(payload.longitude AS Nullable(Float64)) AS longitude
    FROM t_prewhere_guard_json
    WHERE payload.longitude IS NOT NULL
) a
WHERE 1 = 1
  AND pointInPolygon((longitude, latitude)::Point,
        readWKTPolygon('POLYGON ((4.3 52.0,4.4 52.0,4.4 52.1,4.3 52.1,4.3 52.0))')) = 1
SETTINGS allow_reorder_prewhere_conditions = 0;

SELECT 'json guard, query_plan_merge_filters = 0';
SELECT count(DISTINCT session_id)
FROM (
    SELECT session_id,
           CAST(payload.latitude AS Nullable(Float64)) AS latitude,
           CAST(payload.longitude AS Nullable(Float64)) AS longitude
    FROM t_prewhere_guard_json
    WHERE payload.longitude IS NOT NULL
) a
WHERE 1 = 1
  AND pointInPolygon((longitude, latitude)::Point,
        readWKTPolygon('POLYGON ((4.3 52.0,4.4 52.0,4.4 52.1,4.3 52.1,4.3 52.0))')) = 1
SETTINGS query_plan_merge_filters = 0;

DROP TABLE t_prewhere_guard_json;

-- The same shape with a guard that keeps most rows. ReadResult::optimize only applies a step
-- filter on its own when less than 60% of the rows pass, so this case additionally requires the
-- preceding step to force filter materialization.
DROP TABLE IF EXISTS t_prewhere_guard_json_low_sel;
CREATE TABLE t_prewhere_guard_json_low_sel (id Int64, session_id Int64, payload JSON, filler String)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_prewhere_guard_json_low_sel
SELECT number, number,
       if(number % 10 = 0, '{}', '{"longitude":4.35,"latitude":52.06}')::JSON,
       repeat('x', 200)
FROM numbers(100000);

SELECT 'json guard, 90% of rows pass the guard';
SELECT count(DISTINCT session_id)
FROM (
    SELECT session_id,
           CAST(payload.latitude AS Nullable(Float64)) AS latitude,
           CAST(payload.longitude AS Nullable(Float64)) AS longitude
    FROM t_prewhere_guard_json_low_sel
    WHERE payload.longitude IS NOT NULL
) a
WHERE 1 = 1
  AND pointInPolygon((longitude, latitude)::Point,
        readWKTPolygon('POLYGON ((4.3 52.0,4.4 52.0,4.4 52.1,4.3 52.1,4.3 52.0))')) = 1;

SELECT 'json guard, 90% of rows pass the guard, query_plan_merge_filters = 0';
SELECT count(DISTINCT session_id)
FROM (
    SELECT session_id,
           CAST(payload.latitude AS Nullable(Float64)) AS latitude,
           CAST(payload.longitude AS Nullable(Float64)) AS longitude
    FROM t_prewhere_guard_json_low_sel
    WHERE payload.longitude IS NOT NULL
) a
WHERE 1 = 1
  AND pointInPolygon((longitude, latitude)::Point,
        readWKTPolygon('POLYGON ((4.3 52.0,4.4 52.0,4.4 52.1,4.3 52.1,4.3 52.0))')) = 1
SETTINGS query_plan_merge_filters = 0;

DROP TABLE t_prewhere_guard_json_low_sel;

-- The same invariant for Map subcolumns, including a user written PREWHERE.
DROP TABLE IF EXISTS t_prewhere_guard_map;
CREATE TABLE t_prewhere_guard_map (id Int64, tags Map(String, String), filler String)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_prewhere_guard_map
SELECT number,
       if(number % 3 = 0, map('safe', '', 'val', 'not a number'), map('safe', 'y', 'val', toString(number % 100))),
       repeat('x', 200)
FROM numbers(100000);

SELECT 'map guard, WHERE';
SELECT count() FROM t_prewhere_guard_map WHERE tags['safe'] != '' AND toUInt64(tags['val']) >= 0;

SELECT 'map guard, explicit PREWHERE';
SELECT count() FROM t_prewhere_guard_map PREWHERE tags['safe'] != '' AND toUInt64(tags['val']) >= 0;

SELECT 'map guard, WHERE, query_plan_merge_filters = 0';
SELECT count() FROM t_prewhere_guard_map WHERE tags['safe'] != '' AND toUInt64(tags['val']) >= 0
SETTINGS query_plan_merge_filters = 0;

DROP TABLE t_prewhere_guard_map;

-- Conditions that cannot throw are still grouped, so subcolumns of one physical column keep
-- sharing a single read step.
DROP TABLE IF EXISTS t_prewhere_group_map;
CREATE TABLE t_prewhere_group_map (id UInt64, tags Map(String, String))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_prewhere_group_map
SELECT number, mapFromArrays(arrayMap(i -> 'k' || toString(i), range(4)), arrayMap(i -> toString(number + i), range(4)))
FROM numbers(1000);

SELECT 'non throwing map conditions are rewritten to subcolumns and grouped';
SELECT count() = 1 FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_prewhere_group_map
    PREWHERE tags['k0'] != '' AND tags['k1'] != '' AND tags['k2'] != '' AND tags['k3'] != ''
) WHERE explain ILIKE '%tags.key_k0%' AND explain ILIKE '%tags.key_k3%';

SELECT count() FROM t_prewhere_group_map
PREWHERE tags['k0'] != '' AND tags['k1'] != '' AND tags['k2'] != '' AND tags['k3'] != '';

DROP TABLE t_prewhere_group_map;
