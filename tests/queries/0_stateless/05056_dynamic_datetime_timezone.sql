-- This test checks that a DateTime or DateTime64 stored in a Dynamic column renders in the
-- reading session's time zone, and not in the time zone of whichever session happened to read
-- the column first. Parts written with dynamic_serialization_version v1 or v2 persist variant
-- type names as text and rebuild the types when read; the name of a DateTime that declares no
-- time zone carries none, so such a type must be built per read.

DROP TABLE IF EXISTS dynamic_datetime_v2;
DROP TABLE IF EXISTS dynamic_datetime_v3;
DROP TABLE IF EXISTS dynamic_datetime_explicit;
DROP TABLE IF EXISTS dynamic_datetime64_v2;

CREATE TABLE dynamic_datetime_v2 (d Dynamic) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS dynamic_serialization_version = 'v2';
CREATE TABLE dynamic_datetime_v3 (d Dynamic) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS dynamic_serialization_version = 'v3';
CREATE TABLE dynamic_datetime_explicit (d Dynamic) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS dynamic_serialization_version = 'v2';
CREATE TABLE dynamic_datetime64_v2 (d Dynamic) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS dynamic_serialization_version = 'v2';

INSERT INTO dynamic_datetime_v2 SELECT toDateTime(0)::Dynamic FROM numbers(1000)
    SETTINGS session_timezone = 'UTC';
INSERT INTO dynamic_datetime_v3 SELECT toDateTime(0)::Dynamic FROM numbers(1000)
    SETTINGS session_timezone = 'UTC';
INSERT INTO dynamic_datetime_explicit SELECT toDateTime(0, 'Europe/Berlin')::Dynamic FROM numbers(1000)
    SETTINGS session_timezone = 'UTC';
INSERT INTO dynamic_datetime64_v2 SELECT toDateTime64(0, 3)::Dynamic FROM numbers(1000)
    SETTINGS session_timezone = 'UTC';

-- The test runner randomizes dynamic_serialization_version, so confirm the tables kept the
-- versions this test needs. An explicit table setting is never overridden by the runner.
SELECT 'fixture v2', engine_full LIKE '%dynamic_serialization_version = \'v2\'%'
FROM system.tables WHERE database = currentDatabase() AND name = 'dynamic_datetime_v2';
SELECT 'fixture v3', engine_full LIKE '%dynamic_serialization_version = \'v3\'%'
FROM system.tables WHERE database = currentDatabase() AND name = 'dynamic_datetime_v3';

-- Read under Asia/Tokyo first, then UTC, then Asia/Tokyo again. Every read must answer in its
-- own time zone. max_threads = 16 spreads the first read over many threads.
SELECT 'v2 tokyo', any(toString(d)) FROM dynamic_datetime_v2
    SETTINGS session_timezone = 'Asia/Tokyo', max_threads = 16;
SELECT 'v2 utc', any(toString(d)) FROM dynamic_datetime_v2
    SETTINGS session_timezone = 'UTC', max_threads = 16;
SELECT 'v2 tokyo again', any(toString(d)) FROM dynamic_datetime_v2
    SETTINGS session_timezone = 'Asia/Tokyo', max_threads = 16;

-- All rows of one query must render identically, however many threads read them.
SELECT 'v2 renderings per query', count(DISTINCT toString(d)) FROM dynamic_datetime_v2
    SETTINGS session_timezone = 'UTC', max_threads = 16;

-- Control on byte-identical data written with the current default version. It answers in the
-- reading session's time zone both before and after this fix, so a passing v2 arm above is not
-- passing because the two time zones fail to discriminate.
SELECT 'v3 tokyo', any(toString(d)) FROM dynamic_datetime_v3
    SETTINGS session_timezone = 'Asia/Tokyo', max_threads = 16;
SELECT 'v3 utc', any(toString(d)) FROM dynamic_datetime_v3
    SETTINGS session_timezone = 'UTC', max_threads = 16;

-- A variant that declares its own time zone keeps it under every reading session.
SELECT 'explicit tokyo', any(toString(d)) FROM dynamic_datetime_explicit
    SETTINGS session_timezone = 'Asia/Tokyo', max_threads = 16;
SELECT 'explicit utc', any(toString(d)) FROM dynamic_datetime_explicit
    SETTINGS session_timezone = 'UTC', max_threads = 16;

-- DateTime64 is a separate variant name, so it exercises the same path independently.
SELECT 'v2 datetime64 tokyo', any(toString(d)) FROM dynamic_datetime64_v2
    SETTINGS session_timezone = 'Asia/Tokyo', max_threads = 16;
SELECT 'v2 datetime64 utc', any(toString(d)) FROM dynamic_datetime64_v2
    SETTINGS session_timezone = 'UTC', max_threads = 16;

-- Output through a text format resolves the time zone when the value is written, which is a
-- different path from the one above. It answers in the reading session's time zone already.
SELECT 'v2 format tokyo', d FROM dynamic_datetime_v2 LIMIT 1
    SETTINGS session_timezone = 'Asia/Tokyo';
SELECT 'v2 format utc', d FROM dynamic_datetime_v2 LIMIT 1
    SETTINGS session_timezone = 'UTC';

DROP TABLE dynamic_datetime_v2;
DROP TABLE dynamic_datetime_v3;
DROP TABLE dynamic_datetime_explicit;
DROP TABLE dynamic_datetime64_v2;
