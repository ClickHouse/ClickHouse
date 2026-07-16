-- toDate/toDateTime on DateTime64/Date32 overflows for pre-epoch, post-2149, and
-- timezone-shifted boundary values, which broke partition pruning and silently
-- returned incomplete results

DROP TABLE IF EXISTS t_dt64_preepoch;

CREATE TABLE t_dt64_preepoch (
    project_id String,
    id String,
    timestamp DateTime64(3)
) ENGINE = MergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (project_id, timestamp);

INSERT INTO t_dt64_preepoch VALUES ('p1', 'a', '2026-02-21 18:05:58.394');

SELECT 'DateTime64: without filter';
SELECT project_id, id FROM t_dt64_preepoch WHERE project_id = 'p1' AND id = 'a';

SELECT 'DateTime64: with pre-epoch filter';
SELECT project_id, id FROM t_dt64_preepoch WHERE project_id = 'p1' AND id = 'a' AND timestamp >= '1969-12-31 12:00:00';

SELECT 'DateTime64: with epoch filter';
SELECT project_id, id FROM t_dt64_preepoch WHERE project_id = 'p1' AND id = 'a' AND timestamp >= '1970-01-01 00:00:00';

DROP TABLE t_dt64_preepoch;

DROP TABLE IF EXISTS t_date32_preepoch;

CREATE TABLE t_date32_preepoch (
    id String,
    d Date32
) ENGINE = MergeTree()
PARTITION BY toDate(d)
ORDER BY id;

INSERT INTO t_date32_preepoch VALUES ('a', '2026-02-21');

SELECT 'Date32: without filter';
SELECT id FROM t_date32_preepoch WHERE id = 'a';

SELECT 'Date32: with pre-epoch filter';
SELECT id FROM t_date32_preepoch WHERE id = 'a' AND d >= '1969-12-31';

DROP TABLE t_date32_preepoch;

DROP TABLE IF EXISTS t_dt64_upper;

CREATE TABLE t_dt64_upper (
    id UInt8,
    ts DateTime64(0)
) ENGINE = MergeTree()
PARTITION BY toDate(ts)
ORDER BY id;

INSERT INTO t_dt64_upper VALUES (1, '2149-06-07 00:00:00');

SELECT 'DateTime64 upper: data beyond Date max, filter at boundary';
SELECT id FROM t_dt64_upper WHERE ts >= '2149-06-06 00:00:00';

SELECT 'DateTime64 upper: data beyond Date max, filter also beyond';
SELECT id FROM t_dt64_upper WHERE ts >= '2149-06-07 00:00:00';

DROP TABLE t_dt64_upper;

DROP TABLE IF EXISTS t_date32_upper;

CREATE TABLE t_date32_upper (
    id UInt8,
    d Date32
) ENGINE = MergeTree()
PARTITION BY toDate(d)
ORDER BY id;

INSERT INTO t_date32_upper VALUES (1, '2299-12-31');

SELECT 'Date32 upper: data beyond Date max';
SELECT id FROM t_date32_upper WHERE d >= '2149-06-06';

DROP TABLE t_date32_upper;

-- timezone-shifted overflow

DROP TABLE IF EXISTS t_dt64_tz;

CREATE TABLE t_dt64_tz (
    id UInt8,
    ts DateTime64(0, 'UTC')
) ENGINE = MergeTree()
PARTITION BY toDate(ts, 'America/Adak')
ORDER BY id;

INSERT INTO t_dt64_tz VALUES (1, '1970-01-02 00:00:00');

SELECT 'DateTime64 timezone shift: pre-epoch after tz conversion';
SELECT id FROM t_dt64_tz WHERE ts >= '1970-01-01 00:00:00';

DROP TABLE t_dt64_tz;

DROP TABLE IF EXISTS t_dt64_todatetime;

CREATE TABLE t_dt64_todatetime (
    id UInt8,
    ts DateTime64
) ENGINE = MergeTree()
PARTITION BY toDateTime(ts)
ORDER BY id;

INSERT INTO t_dt64_todatetime VALUES (1, '2026-02-21 00:00:00');

SELECT 'toDateTime(DateTime64): pre-epoch filter';
SELECT groupArray(id) FROM t_dt64_todatetime WHERE ts >= '1969-12-31 12:00:00';

DROP TABLE t_dt64_todatetime;

DROP TABLE IF EXISTS test;
CREATE TABLE test (id UInt8, ts DateTime64(0))
ENGINE=MergeTree PARTITION BY toUnixTimestamp(ts) ORDER BY id;
INSERT INTO test VALUES (1, '2026-02-21 10:00:00');

SELECT id FROM test WHERE ts >= '1969-12-31 12:00:00';
DROP TABLE IF EXISTS test;

CREATE TABLE test (id UInt8, ts DateTime64(0))
ENGINE=MergeTree PARTITION BY toUnixTimestamp(ts) ORDER BY id;
INSERT INTO test VALUES (1, '2026-02-21 10:00:00');

SELECT id FROM test WHERE ts >= '2107-01-01 00:00:00';
DROP TABLE IF EXISTS test;

DROP TABLE IF EXISTS test_d32_uint32;
CREATE TABLE test_d32_uint32 (id UInt8, d Date32)
ENGINE = MergeTree PARTITION BY toUnixTimestamp(d) ORDER BY id;
INSERT INTO test_d32_uint32 VALUES (1, '2026-02-21');
SELECT 'date32_pre_epoch';
SELECT count() FROM test_d32_uint32 WHERE d >= '1960-01-01';
DROP TABLE test_d32_uint32;

DROP TABLE IF EXISTS test_time64_uint32;
CREATE TABLE test_time64_uint32 (id UInt8, t Time64(0))
ENGINE = MergeTree PARTITION BY toUnixTimestamp(t) ORDER BY id;
INSERT INTO test_time64_uint32 VALUES (1, '10:00:00');
SELECT 'time64_negative';
SELECT count() FROM test_time64_uint32 WHERE t >= '-01:00:00';
DROP TABLE test_time64_uint32;
