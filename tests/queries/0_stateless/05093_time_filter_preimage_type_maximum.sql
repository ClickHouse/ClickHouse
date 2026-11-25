-- https://github.com/ClickHouse/ClickHouse/issues/116902
-- `optimize_time_filter_with_preimage` rewrites `toYear(x) <op> c` into range comparisons against the
-- year boundaries, rendered as untyped literals that the comparison re-parses against the column
-- type. An endpoint the column type cannot hold saturates on that re-parse, and the rewrite compares
-- strictly against the exclusive upper endpoint, so the boundary rows used to flip. The preimage is
-- now declined when an endpoint does not fit the column type.

SET session_timezone = 'UTC';
-- The flaky check randomizes `optimize_time_filter_with_preimage`; the checks below need it on.
SET optimize_time_filter_with_preimage = 1;
-- The analyzer is not pinned session-wide on purpose: the result checks then cover the old AST
-- optimizer too on the CI run that turns the analyzer off. Only the `EXPLAIN QUERY TREE` checks,
-- which exist in the analyzer alone, pin it per query.

DROP TABLE IF EXISTS t_preimage_date;
CREATE TABLE t_preimage_date (d Date) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_preimage_date VALUES ('2148-12-31'),('2149-01-01'),('2149-06-05'),('2149-06-06');

SELECT count() FROM t_preimage_date WHERE toYear(d) = 2149;
SELECT count() FROM t_preimage_date WHERE toYear(d) = 2149 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT count() FROM t_preimage_date WHERE toYear(d) != 2149;
SELECT count() FROM t_preimage_date WHERE toYear(d) != 2149 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT count() FROM t_preimage_date WHERE toYear(d) >= 2150;
SELECT count() FROM t_preimage_date WHERE toYear(d) > 2149;
SELECT count() FROM t_preimage_date WHERE toYear(d) <= 2149;
SELECT count() FROM t_preimage_date WHERE toYear(d) < 2150;
SELECT count() FROM t_preimage_date WHERE toYYYYMM(d) = 214906;
SELECT count() FROM t_preimage_date WHERE toYYYYMM(d) = 214906 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT count() FROM t_preimage_date WHERE toYYYYMM(d) != 214906;

SELECT 'DateTime at its maximum';
DROP TABLE IF EXISTS t_preimage_datetime;
CREATE TABLE t_preimage_datetime (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_preimage_datetime VALUES ('2105-12-31 23:59:59'),('2106-01-01 00:00:00'),('2106-02-07 06:28:14'),('2106-02-07 06:28:15');
SELECT count() FROM t_preimage_datetime WHERE toYear(dt) = 2106;
SELECT count() FROM t_preimage_datetime WHERE toYear(dt) = 2106 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT count() FROM t_preimage_datetime WHERE toYear(dt) >= 2107;
SELECT count() FROM t_preimage_datetime WHERE toYYYYMM(dt) = 210602;

SELECT 'DateTime64 no longer throws';
DROP TABLE IF EXISTS t_preimage_datetime64;
CREATE TABLE t_preimage_datetime64 (x DateTime64(9,'UTC')) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_preimage_datetime64 VALUES ('2262-01-01 00:00:00.000000000'),('2262-04-11 23:47:16.854775807');
SELECT count() FROM t_preimage_datetime64 WHERE toYear(x) = 2262;
SELECT count() FROM t_preimage_datetime64 WHERE toYear(x) = 2262 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT count() FROM t_preimage_datetime64 WHERE toYear(x) <= 2262;
SELECT count() FROM t_preimage_datetime64 WHERE toYYYYMM(x) = 226204;

SELECT 'the rewrite still applies away from the type maximum';
DROP TABLE IF EXISTS t_preimage_ordinary;
CREATE TABLE t_preimage_ordinary (d Date) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_preimage_ordinary VALUES ('2019-12-31'),('2020-01-01'),('2020-06-15'),('2020-12-31'),('2021-01-01');
SELECT count() FROM t_preimage_ordinary WHERE toYear(d) = 2020;
SELECT count() FROM t_preimage_ordinary WHERE toYear(d) = 2020 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_preimage_ordinary WHERE toYear(d) = 2020) WHERE explain LIKE '%2021-01-01%' SETTINGS enable_analyzer = 1;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_preimage_date WHERE toYear(d) = 2149) WHERE explain LIKE '%2150-01-01%' SETTINGS enable_analyzer = 1;

SELECT 'Date32 spans the whole representable range';
DROP TABLE IF EXISTS t_preimage_date32;
CREATE TABLE t_preimage_date32 (d Date32) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_preimage_date32 VALUES ('2299-12-31'),('9999-12-30'),('9999-12-31');
SELECT count() FROM t_preimage_date32 WHERE toYear(d) = 9999;
SELECT count() FROM t_preimage_date32 WHERE toYear(d) = 9999 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT count() FROM t_preimage_date32 WHERE toYear(d) = 2299;

DROP TABLE t_preimage_date;
DROP TABLE t_preimage_datetime;
DROP TABLE t_preimage_datetime64;
DROP TABLE t_preimage_ordinary;
DROP TABLE t_preimage_date32;
