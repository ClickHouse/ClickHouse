-- A `DateTime` that declares no time zone renders in the reading session's one. Its serialization
-- resolves `session_timezone` when built and keeps that zone, while the type name stays bare, so
-- such a serialization must not be shared by name between queries, and a `Dynamic` column fetches
-- one by name on every read. Every statement pins `session_timezone`, which the runner randomizes.

DROP TABLE IF EXISTS t_datetime_dynamic;
DROP TABLE IF EXISTS t_datetime_dynamic_explicit;

-- v2 parts persist a Dynamic column's variant types as names and rebuild them on each read, so the
-- variant's serialization is fetched by name. v3 encodes the types instead; both must be correct.
CREATE TABLE t_datetime_dynamic (d Dynamic) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS dynamic_serialization_version = 'v2';
CREATE TABLE t_datetime_dynamic_explicit (d Dynamic) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS dynamic_serialization_version = 'v2';

-- `toDateTime(0)` declares no zone, so the variant is stored as a bare `DateTime` and only its value
-- comes from the inserting session. Naming a zone would store `DateTime('UTC')`, which keeps it.
INSERT INTO t_datetime_dynamic SELECT toDateTime(0)::Dynamic FROM numbers(1000)
    SETTINGS session_timezone = 'UTC';
INSERT INTO t_datetime_dynamic_explicit SELECT toDateTime(0, 'Europe/Berlin')::Dynamic FROM numbers(1000)
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
