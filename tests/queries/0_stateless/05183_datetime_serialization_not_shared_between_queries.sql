-- A `DateTime` that declares no time zone renders in the reading session's one. Its serialization
-- resolves `session_timezone` when built and keeps that zone, while the type name stays bare, so
-- such a serialization must not be shared by name between queries. `Dynamic` and `JSON` fetch one
-- by name on every read. Every statement pins `session_timezone`, which the test runner randomizes.

DROP TABLE IF EXISTS t_datetime_dynamic;
DROP TABLE IF EXISTS t_datetime_dynamic_explicit;
DROP TABLE IF EXISTS t_datetime_json;

-- v2 parts persist a Dynamic column's variant types as names and rebuild them on each read, so the
-- variant's serialization is fetched by name. v3 encodes the types instead; both must be correct.
CREATE TABLE t_datetime_dynamic (d Dynamic) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS dynamic_serialization_version = 'v2';
CREATE TABLE t_datetime_dynamic_explicit (d Dynamic) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS dynamic_serialization_version = 'v2';
CREATE TABLE t_datetime_json (j JSON(ts DateTime)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_datetime_dynamic SELECT toDateTime(0, 'UTC')::Dynamic FROM numbers(1000);
INSERT INTO t_datetime_dynamic_explicit SELECT toDateTime(0, 'Europe/Berlin')::Dynamic FROM numbers(1000);
INSERT INTO t_datetime_json SELECT '{"ts": "1970-01-01 00:00:00"}' FROM numbers(1000)
    SETTINGS session_timezone = 'UTC';

-- Alternating zones on one thread: with a shared serialization a read is answered in the zone of
-- whichever query built it first, so the zone that comes second is the one that shows it.
SELECT 'dynamic tokyo', any(toString(d)) FROM t_datetime_dynamic
    SETTINGS session_timezone = 'Asia/Tokyo', max_threads = 1;
SELECT 'dynamic utc', any(toString(d)) FROM t_datetime_dynamic
    SETTINGS session_timezone = 'UTC', max_threads = 1;
SELECT 'dynamic tokyo again', any(toString(d)) FROM t_datetime_dynamic
    SETTINGS session_timezone = 'Asia/Tokyo', max_threads = 1;
SELECT 'dynamic utc again', any(toString(d)) FROM t_datetime_dynamic
    SETTINGS session_timezone = 'UTC', max_threads = 1;

-- All rows of one query render identically however many threads read them.
SELECT 'dynamic renderings per query', count(DISTINCT toString(d)) FROM t_datetime_dynamic
    SETTINGS session_timezone = 'Asia/Tokyo', max_threads = 16;

-- A `JSON` typed path holds its own serialization, so it is reached only if the lookup also
-- reports the types nested inside a composite one.
SELECT 'json tokyo', any(toString(j.ts)) FROM t_datetime_json
    SETTINGS session_timezone = 'Asia/Tokyo', max_threads = 1;
SELECT 'json utc', any(toString(j.ts)) FROM t_datetime_json
    SETTINGS session_timezone = 'UTC', max_threads = 1;
SELECT 'json tokyo again', any(toString(j.ts)) FROM t_datetime_json
    SETTINGS session_timezone = 'Asia/Tokyo', max_threads = 1;

-- A declared zone belongs to the value and survives every reading session.
SELECT 'explicit tokyo', any(toString(d)) FROM t_datetime_dynamic_explicit
    SETTINGS session_timezone = 'Asia/Tokyo', max_threads = 1;
SELECT 'explicit utc', any(toString(d)) FROM t_datetime_dynamic_explicit
    SETTINGS session_timezone = 'UTC', max_threads = 1;

-- Output through a text format goes by a different route and must agree with the above.
SELECT 'dynamic format tokyo', d FROM t_datetime_dynamic LIMIT 1
    SETTINGS session_timezone = 'Asia/Tokyo';
SELECT 'dynamic format utc', d FROM t_datetime_dynamic LIMIT 1
    SETTINGS session_timezone = 'UTC';

DROP TABLE t_datetime_dynamic;
DROP TABLE t_datetime_dynamic_explicit;
DROP TABLE t_datetime_json;
