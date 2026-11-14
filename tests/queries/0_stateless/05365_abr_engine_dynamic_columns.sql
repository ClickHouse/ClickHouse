-- Tags: no-parallel
-- no-parallel: creates a global user with a fixed name, as `EXECUTE AS` takes no query parameter

-- An `ABR` table can be created over source tables with `JSON` and `Dynamic` columns, also nested in other types, and
-- reads them, as full columns and as subcolumns.

SET allow_experimental_abr = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS src_1;
DROP TABLE IF EXISTS src_10;

CREATE TABLE src_1 (ts DateTime, payload JSON, d Dynamic, t Tuple(x Dynamic))
ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
CREATE TABLE src_10 AS src_1;

INSERT INTO src_1 VALUES
    ('2026-01-01 00:00:00', '{"a": "x", "b": {"c": "y"}}', 1, tuple(2)),
    ('2026-01-02 00:00:00', '{"a": "z"}', 'str', tuple('s'));

CREATE TABLE abr ENGINE = ABR('src', [1, 10], ('ts'));

SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'abr' ORDER BY position;

SELECT 'full columns';
SELECT ts, payload, d, t FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00') ORDER BY ts;

SELECT 'subcolumns';
SELECT payload.a, payload.b.c, d.Int64, d.String, dynamicType(t.x) FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00') ORDER BY ts;

SELECT 'subcolumns, old analyzer';
SELECT payload.a, d.Int64, d.String FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00') ORDER BY ts SETTINGS enable_analyzer = 0;

SELECT 'subcolumns, grants on their columns';
DROP USER IF EXISTS user_05365;
CREATE USER user_05365;
GRANT SELECT(ts, payload, d, t) ON abr TO user_05365;
GRANT SELECT(ts, payload, d, t) ON src_1 TO user_05365;
GRANT SELECT ON src_10 TO user_05365;
EXECUTE AS user_05365 SELECT payload.a, d.Int64, dynamicType(t.x) FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00') ORDER BY ts;
-- A revoke on the subcolumn itself still applies, on every source table the query may be executed on.
REVOKE SELECT(`t.x`) ON src_10 FROM user_05365;
EXECUTE AS user_05365 SELECT dynamicType(t.x) FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00'); -- { serverError ACCESS_DENIED }
DROP USER user_05365;

DROP TABLE abr;
DROP TABLE src_1;
DROP TABLE src_10;
