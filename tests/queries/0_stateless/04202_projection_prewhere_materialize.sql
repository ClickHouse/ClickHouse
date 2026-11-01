-- Test for issues #104117, #104235, #104256
-- Regression from PR #88798: removeTrivialWrappers in appendExpression
-- breaks incremental DAG merging when materialize() wrappers are stripped
-- before the full DAG is assembled.

SET optimize_use_projections = 1;
SET force_optimize_projection = 1;

-- Issue #104117: NOT_FOUND_COLUMN_IN_BLOCK with UNION ALL view + projection + WHERE on aggregated column
DROP TABLE IF EXISTS t1_04202;
DROP TABLE IF EXISTS t2_04202;
DROP VIEW IF EXISTS v_04202;

CREATE TABLE t1_04202 (
    id Int64,
    ts DateTime64(6),
    grp String,
    val Int32,
    PROJECTION proj_filter (SELECT grp, ts, val ORDER BY grp, ts)
) ENGINE = MergeTree ORDER BY ts;

CREATE TABLE t2_04202 (
    id Int64,
    ts DateTime64(6),
    grp String,
    val Int32,
    PROJECTION proj_filter (SELECT grp, ts, val ORDER BY grp, ts)
) ENGINE = MergeTree ORDER BY ts;

INSERT INTO t1_04202 VALUES (1, '2026-04-01 00:00:00', 'a', 5), (2, '2026-04-02 00:00:00', 'b', 10);
INSERT INTO t2_04202 VALUES (3, '2026-02-01 00:00:00', 'a', 15), (4, '2026-02-02 00:00:00', 'b', 20);

CREATE VIEW v_04202 AS
SELECT toInt64(0) AS id, ts, grp, val FROM t1_04202 WHERE ts >= '2026-03-05 21:00:00'
UNION ALL
SELECT id, ts, grp, val FROM t2_04202 WHERE ts < '2026-03-05 21:00:00';

SELECT avg(val) FROM v_04202 WHERE val > 0;

DROP VIEW v_04202;
DROP TABLE t1_04202;
DROP TABLE t2_04202;

-- Issue #104235: Block structure mismatch with window functions + projection + WHERE
DROP TABLE IF EXISTS t_window_04202;

CREATE TABLE t_window_04202 (
    id UUID,
    session_id UUID,
    role String,
    ts DateTime64(6),
    PROJECTION proj_session_id (SELECT * ORDER BY session_id)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_window_04202 VALUES
    (generateUUIDv4(), generateUUIDv4(), 'assistant', '2026-01-01 00:00:00'),
    (generateUUIDv4(), generateUUIDv4(), 'user',      '2026-01-01 00:00:01');

SELECT
    session_id,
    ts,
    row_number() OVER (PARTITION BY session_id ORDER BY ts) AS rn
FROM t_window_04202
WHERE role = 'assistant'
SETTINGS query_plan_remove_unused_columns = 0
FORMAT Null;

DROP TABLE t_window_04202;

-- Issue #104256: AMBIGUOUS_COLUMN_NAME when alias collides with source column + projection
-- The regression is the exception itself (AMBIGUOUS_COLUMN_NAME / block structure mismatch),
-- not the empty result — the empty result is pre-existing analyzer behavior where alias
-- shadows the column name in WHERE clause. Use prefer_column_name_to_alias = 1 to fix that.
DROP TABLE IF EXISTS t_alias_04202;

CREATE TABLE t_alias_04202 (
    id UInt64,
    session_id UUID,
    role String,
    PROJECTION proj_session_id (SELECT * ORDER BY session_id)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_alias_04202 VALUES
    (1, 'a4f08805-17ee-44bd-bd12-53a909221f5e', 'assistant'),
    (2, generateUUIDv4(), 'user');

-- Without prefer_column_name_to_alias, alias shadows column in WHERE → empty result (pre-existing).
-- Previously threw AMBIGUOUS_COLUMN_NAME with projection + query_plan_remove_unused_columns = 0.
SELECT toString(session_id) AS session_id
FROM t_alias_04202
WHERE session_id IN _CAST(['a4f08805-17ee-44bd-bd12-53a909221f5e'], 'Array(UUID)')
ORDER BY role
SETTINGS query_plan_remove_unused_columns = 0
FORMAT Null;

-- With prefer_column_name_to_alias = 1, WHERE references the source column correctly.
SELECT toString(session_id) AS session_id
FROM t_alias_04202
WHERE session_id IN _CAST(['a4f08805-17ee-44bd-bd12-53a909221f5e'], 'Array(UUID)')
ORDER BY role
SETTINGS query_plan_remove_unused_columns = 0, prefer_column_name_to_alias = 1;

DROP TABLE t_alias_04202;
