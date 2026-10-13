-- `optimize_time_filter_with_preimage` renders the preimage endpoints as untyped literals that the
-- comparison re-parses against the column type. `readDateTime64Text` maps every literal whose year
-- component is `0000` to the Unix epoch instead of year zero, so a `DateTime64` boundary in year zero
-- does not round-trip and the rewrite would compare against 1970. The preimage is declined there.
-- The extended calendar starts at `0000-01-01`, so only `toYear(x) = 0` and `toYYYYMM(x) = 1` to `12`
-- can produce such a boundary, and only for `DateTime64`: `Date` and `DateTime` start at 1970, and
-- `Date32` literals go through a day-number parser that has no year-zero special case.

-- No `enable_analyzer` pin here: this test asserts on results only, so it covers the old AST
-- optimizer as well as the analyzer pass.
SET session_timezone = 'UTC';
-- The flaky check randomizes the setting under test.
SET optimize_time_filter_with_preimage = 1;

DROP TABLE IF EXISTS t_preimage_year_zero;
CREATE TABLE t_preimage_year_zero (x DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY x;

-- Year zero cannot be written as a literal for the very reason this test exists, so count back from
-- year one instead. Year zero is a leap year in the proleptic Gregorian calendar.
INSERT INTO t_preimage_year_zero SELECT toDateTime64('0001-01-01 00:00:00', 0, 'UTC') - 366 * 86400;
INSERT INTO t_preimage_year_zero SELECT toDateTime64('0001-01-01 00:00:00', 0, 'UTC') - 86400;
INSERT INTO t_preimage_year_zero VALUES ('0001-01-01 00:00:00'), ('1970-01-01 00:00:00');

SELECT x, toYear(x), toYYYYMM(x) FROM t_preimage_year_zero ORDER BY x;

SELECT 'toYear';
SELECT count() FROM t_preimage_year_zero WHERE toYear(x) = 0;
SELECT count() FROM t_preimage_year_zero WHERE toYear(x) = 0 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT count() FROM t_preimage_year_zero WHERE toYear(x) != 0;
SELECT count() FROM t_preimage_year_zero WHERE toYear(x) != 0 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT count() FROM t_preimage_year_zero WHERE toYear(x) >= 1;
SELECT count() FROM t_preimage_year_zero WHERE toYear(x) >= 1 SETTINGS optimize_time_filter_with_preimage = 0;

SELECT 'toYYYYMM';
SELECT count() FROM t_preimage_year_zero WHERE toYYYYMM(x) = 1;
SELECT count() FROM t_preimage_year_zero WHERE toYYYYMM(x) = 1 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT count() FROM t_preimage_year_zero WHERE toYYYYMM(x) = 12;
SELECT count() FROM t_preimage_year_zero WHERE toYYYYMM(x) = 12 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT count() FROM t_preimage_year_zero WHERE toYYYYMM(x) = 197001;
SELECT count() FROM t_preimage_year_zero WHERE toYYYYMM(x) = 197001 SETTINGS optimize_time_filter_with_preimage = 0;

SELECT 'Date32 has no year-zero special case, so the rewrite still applies';
DROP TABLE IF EXISTS t_preimage_year_zero_date32;
CREATE TABLE t_preimage_year_zero_date32 (d Date32) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_preimage_year_zero_date32 SELECT toDate32('0001-01-01') - 366;
INSERT INTO t_preimage_year_zero_date32 VALUES ('0001-01-01'), ('1970-01-01');
SELECT count() FROM t_preimage_year_zero_date32 WHERE toYear(d) = 0;
SELECT count() FROM t_preimage_year_zero_date32 WHERE toYear(d) = 0 SETTINGS optimize_time_filter_with_preimage = 0;
SELECT count() FROM t_preimage_year_zero_date32 WHERE toYYYYMM(d) = 1;
SELECT count() FROM t_preimage_year_zero_date32 WHERE toYYYYMM(d) = 1 SETTINGS optimize_time_filter_with_preimage = 0;

DROP TABLE t_preimage_year_zero;
DROP TABLE t_preimage_year_zero_date32;
