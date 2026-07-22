-- Correctness of the fast `MODIFY TTL` optimization. The fast path only shifts each part's stored TTL
-- timestamps by a constant, so it must:
--   * handle every TTL result type (Date, DateTime, Date32, DateTime64) without a logical error;
--   * fall back to a full rewrite when the delta is not a constant number of seconds (calendar
--     month/year intervals, DST-sensitive intervals, column-dependent expressions);
--   * fall back for parts whose stored TTL info is stale (left by materialize_ttl_after_modify = 0),
--     never deleting rows that are not actually expired.
-- In every case the resulting row set must be correct.

SET alter_sync = 2;
SET allow_suspicious_ttl_expressions = 1;

SELECT 'Date32 TTL shortened so every row expires';
DROP TABLE IF EXISTS t_ttl_date32;
CREATE TABLE t_ttl_date32 (id UInt32, d Date32) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 300 DAY;
INSERT INTO t_ttl_date32 SELECT number, today() - 100 FROM numbers(1000);
SELECT count() FROM t_ttl_date32;
ALTER TABLE t_ttl_date32 MODIFY TTL d + INTERVAL 10 DAY;
SELECT count() FROM t_ttl_date32;
DROP TABLE t_ttl_date32;

SELECT 'DateTime64 TTL shortened so every row expires';
DROP TABLE IF EXISTS t_ttl_dt64;
CREATE TABLE t_ttl_dt64 (id UInt32, d DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 300 DAY;
INSERT INTO t_ttl_dt64 SELECT number, now64(3, 'UTC') - INTERVAL 100 DAY FROM numbers(1000);
ALTER TABLE t_ttl_dt64 MODIFY TTL d + INTERVAL 10 DAY;
SELECT count() FROM t_ttl_dt64;
DROP TABLE t_ttl_dt64;

SELECT 'MODIFY TTL whose result type becomes DateTime64 must not throw (was a logical error)';
DROP TABLE IF EXISTS t_ttl_ns;
CREATE TABLE t_ttl_ns (id UInt32, d DateTime) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 300 DAY;
INSERT INTO t_ttl_ns SELECT number, now() - INTERVAL 500 DAY FROM numbers(10);
ALTER TABLE t_ttl_ns MODIFY TTL d + INTERVAL 1 NANOSECOND;
SELECT count() FROM t_ttl_ns;
DROP TABLE t_ttl_ns;

SELECT 'Calendar month interval falls back and still expires the right rows';
DROP TABLE IF EXISTS t_ttl_month;
CREATE TABLE t_ttl_month (id UInt32, d DateTime('UTC')) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 12 MONTH;
INSERT INTO t_ttl_month SELECT number, now('UTC') - INTERVAL 11 MONTH FROM numbers(1000);
ALTER TABLE t_ttl_month MODIFY TTL d + INTERVAL 10 MONTH;
SELECT count() FROM t_ttl_month;
DROP TABLE t_ttl_month;

SELECT 'Column-dependent TTL falls back and still expires the right rows';
DROP TABLE IF EXISTS t_ttl_if;
-- Start with a TTL that keeps every row (so nothing is dropped at INSERT time), then shorten it only
-- for even ids. A constant per-part shift would wrongly drop every row here; the fallback keeps the odd
-- ids. `id` is a non-date column, so the fast path is not eligible and a full rewrite is used.
CREATE TABLE t_ttl_if (id UInt32, d DateTime('UTC')) ENGINE = MergeTree ORDER BY id
    TTL d + toIntervalDay(if(id % 2 = 0, 1000, 2000));
INSERT INTO t_ttl_if SELECT number, now('UTC') FROM numbers(1000);
ALTER TABLE t_ttl_if MODIFY TTL d + toIntervalDay(if(id % 2 = 0, -1000, 2000));
SELECT count() FROM t_ttl_if;
DROP TABLE t_ttl_if;

SELECT 'Stale part TTL info (materialize_ttl_after_modify = 0) must not delete live rows';
DROP TABLE IF EXISTS t_ttl_stale;
CREATE TABLE t_ttl_stale (id UInt32, d DateTime('UTC')) ENGINE = MergeTree ORDER BY id TTL d + INTERVAL 1 DAY;
INSERT INTO t_ttl_stale SELECT number, now('UTC') FROM numbers(1000);
ALTER TABLE t_ttl_stale MODIFY TTL d + INTERVAL 300 DAY SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE t_ttl_stale MODIFY TTL d + INTERVAL 290 DAY;
SELECT count() FROM t_ttl_stale;
DROP TABLE t_ttl_stale;
