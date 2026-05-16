-- Verify that TTL expressions on `Date` / `DateTime` (16/32-bit) columns are evaluated
-- in widened types (`Date32` / `DateTime64`) so arithmetic with large intervals does not
-- silently wrap. Previously, `day + toIntervalDay(46000)` or `ts + INTERVAL 100 YEAR`
-- would overflow, corrupt part min/max TTL metadata, and drop entire parts during merge.
--
-- The fix widens `Date` -> `Date32` and `DateTime` -> `DateTime64(0, tz)` in the
-- column list passed to the TTL expression analyzer, so arithmetic runs in the 64-bit
-- domain. At execution time the narrow block columns are cast to the widened types
-- before the expression runs.

-- Case 1: rows TTL with a `Date` column and a >100-year interval.
DROP TABLE IF EXISTS t_ttl_overflow;
CREATE TABLE t_ttl_overflow (day Date, value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_overflow VALUES ('2024-01-01', 1), ('2024-06-15', 2), ('2025-12-31', 3);
ALTER TABLE t_ttl_overflow MODIFY TTL day + toIntervalDay(46000);
OPTIMIZE TABLE t_ttl_overflow FINAL;
-- TTL evaluates to year ~2150; data is retained.
SELECT count() FROM t_ttl_overflow;
DROP TABLE t_ttl_overflow;

-- Case 2: same with `materialize_ttl_after_modify = 0` then an explicit `MATERIALIZE TTL`.
DROP TABLE IF EXISTS t_ttl_overflow2;
CREATE TABLE t_ttl_overflow2 (day Date, value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_overflow2 VALUES ('2024-01-01', 10), ('2024-06-15', 20);
SET materialize_ttl_after_modify = 0;
ALTER TABLE t_ttl_overflow2 MODIFY TTL day + toIntervalDay(46000);
SELECT count() FROM t_ttl_overflow2;
SET mutations_sync = 1;
ALTER TABLE t_ttl_overflow2 MATERIALIZE TTL;
SELECT count() FROM t_ttl_overflow2;
SET materialize_ttl_after_modify = 1;
SET mutations_sync = 0;
DROP TABLE t_ttl_overflow2;

-- Case 3: every `add*` granularity must widen for `Date` columns.
DROP TABLE IF EXISTS t_ttl_date_fns;
CREATE TABLE t_ttl_date_fns (day Date, value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_date_fns VALUES ('2024-01-01', 1), ('2024-06-15', 2), ('2025-12-31', 3);

ALTER TABLE t_ttl_date_fns MODIFY TTL day + INTERVAL 200 YEAR;
SELECT count() FROM t_ttl_date_fns;
ALTER TABLE t_ttl_date_fns MODIFY TTL addYears(day, 150);
SELECT count() FROM t_ttl_date_fns;
ALTER TABLE t_ttl_date_fns MODIFY TTL addMonths(day, 2000);
SELECT count() FROM t_ttl_date_fns;
ALTER TABLE t_ttl_date_fns MODIFY TTL addWeeks(day, 6600);
SELECT count() FROM t_ttl_date_fns;
ALTER TABLE t_ttl_date_fns MODIFY TTL addDays(day, 46000);
SELECT count() FROM t_ttl_date_fns;
DROP TABLE t_ttl_date_fns;

-- Case 4: every `add*` granularity must widen for `DateTime` columns.
DROP TABLE IF EXISTS t_ttl_datetime_fns;
CREATE TABLE t_ttl_datetime_fns (ts DateTime, value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_datetime_fns VALUES ('2034-01-01 00:00:00', 1), ('2034-06-15 12:00:00', 2), ('2035-12-31 23:59:59', 3);

ALTER TABLE t_ttl_datetime_fns MODIFY TTL ts + INTERVAL 100 YEAR;
SELECT count() FROM t_ttl_datetime_fns;
ALTER TABLE t_ttl_datetime_fns MODIFY TTL addYears(ts, 100);
SELECT count() FROM t_ttl_datetime_fns;
ALTER TABLE t_ttl_datetime_fns MODIFY TTL addMonths(ts, 1200);
SELECT count() FROM t_ttl_datetime_fns;
ALTER TABLE t_ttl_datetime_fns MODIFY TTL addWeeks(ts, 5200);
SELECT count() FROM t_ttl_datetime_fns;
ALTER TABLE t_ttl_datetime_fns MODIFY TTL addDays(ts, 36500);
SELECT count() FROM t_ttl_datetime_fns;
ALTER TABLE t_ttl_datetime_fns MODIFY TTL addHours(ts, 876000);
SELECT count() FROM t_ttl_datetime_fns;
ALTER TABLE t_ttl_datetime_fns MODIFY TTL addMinutes(ts, 52560000);
SELECT count() FROM t_ttl_datetime_fns;
ALTER TABLE t_ttl_datetime_fns MODIFY TTL addSeconds(ts, 3153600000);
SELECT count() FROM t_ttl_datetime_fns;
DROP TABLE t_ttl_datetime_fns;

-- Case 5: column TTL (`TTLColumnAlgorithm`).
DROP TABLE IF EXISTS t_ttl_col_algo;
CREATE TABLE t_ttl_col_algo (day Date, val String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_col_algo VALUES ('2024-01-01', 'hello'), ('2024-06-15', 'world');
ALTER TABLE t_ttl_col_algo MODIFY COLUMN val String TTL day + toIntervalDay(46000);
SELECT val FROM t_ttl_col_algo ORDER BY val;
DROP TABLE t_ttl_col_algo;

-- Case 6: GROUP BY TTL (`TTLAggregationAlgorithm`).
DROP TABLE IF EXISTS t_ttl_groupby_algo;
CREATE TABLE t_ttl_groupby_algo (day Date, key UInt64, val UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_ttl_groupby_algo VALUES ('2024-01-01', 1, 10), ('2024-01-01', 1, 20), ('2024-06-15', 2, 30);
ALTER TABLE t_ttl_groupby_algo MODIFY TTL day + toIntervalDay(46000) GROUP BY key SET val = max(val);
-- Aggregation does not run because TTL is in the future; row count stays 3.
SELECT count() FROM t_ttl_groupby_algo;
DROP TABLE t_ttl_groupby_algo;

-- Case 7: RECOMPRESS TTL (`TTLUpdateInfoAlgorithm`). `materialize_ttl_after_modify`
-- is disabled here because `MATERIALIZE TTL` expects a rows TTL — recompression-only
-- TTLs don't have one, and the auto-fired materialization mutation would fail flakily
-- under TSan with `Cannot MATERIALIZE TTL as there is no TTL set`.
DROP TABLE IF EXISTS t_ttl_recompress_algo;
CREATE TABLE t_ttl_recompress_algo (day Date, val UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_recompress_algo VALUES ('2024-01-01', 1), ('2024-06-15', 2);
SET materialize_ttl_after_modify = 0;
ALTER TABLE t_ttl_recompress_algo MODIFY TTL day + toIntervalDay(46000) RECOMPRESS CODEC(ZSTD(1));
SET materialize_ttl_after_modify = 1;
SELECT count() FROM t_ttl_recompress_algo;
DROP TABLE t_ttl_recompress_algo;

-- LowCardinality on temporal types is not on by default — these cases need the
-- explicit opt-in.
SET allow_suspicious_low_cardinality_types = 1;

-- Case 8: `LowCardinality(Date)` — widening must apply after unwrapping the LC layer.
DROP TABLE IF EXISTS t_ttl_lc_date;
CREATE TABLE t_ttl_lc_date (day LowCardinality(Date), value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_lc_date VALUES ('2024-01-01', 1), ('2024-06-15', 2), ('2025-12-31', 3);
ALTER TABLE t_ttl_lc_date MODIFY TTL day + toIntervalDay(46000);
SELECT count() FROM t_ttl_lc_date;
DROP TABLE t_ttl_lc_date;

-- Case 9: `LowCardinality(DateTime)` — same check for the 32-bit timestamp path.
DROP TABLE IF EXISTS t_ttl_lc_datetime;
CREATE TABLE t_ttl_lc_datetime (ts LowCardinality(DateTime), value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_lc_datetime VALUES ('2034-01-01 00:00:00', 1), ('2034-06-15 12:00:00', 2);
ALTER TABLE t_ttl_lc_datetime MODIFY TTL ts + INTERVAL 100 YEAR;
SELECT count() FROM t_ttl_lc_datetime;
DROP TABLE t_ttl_lc_datetime;

SET allow_suspicious_low_cardinality_types = 0;

-- Case 10: a lambda parameter that shadows a table column must keep its lambda-local
-- type. The TTL expression contains `arrayFilter(day -> day != '', arr)` where the
-- lambda's `day` is a String element of `arr`, not the `Date` column `day`. Because the
-- widening happens at column-type level (not AST rewriting), the analyzer's normal name
-- resolution already binds `day` inside the lambda to the lambda parameter rather than
-- to the outer column, so this case is handled automatically.
DROP TABLE IF EXISTS t_ttl_lambda_shadow;
CREATE TABLE t_ttl_lambda_shadow (day Date, arr Array(String), value UInt64) ENGINE = MergeTree ORDER BY tuple();
-- Future dates so the resulting TTL is not yet expired — the count assertion below then
-- meaningfully checks that the ALTER succeeded and rows survive (rather than collapsing
-- to 0 from immediate expiration).
INSERT INTO t_ttl_lambda_shadow VALUES ('2099-01-01', ['a', 'b'], 1), ('2099-06-15', ['', 'c'], 2);
ALTER TABLE t_ttl_lambda_shadow
    MODIFY TTL day + toIntervalDay(length(arrayFilter(day -> day != '', arr)));
SELECT count() FROM t_ttl_lambda_shadow;
DROP TABLE t_ttl_lambda_shadow;

-- Case 11: negative control — normal TTL expiration must still drop expired rows.
-- Year 2000 + 1 day is far in the past, so all rows should be removed at merge time.
DROP TABLE IF EXISTS t_ttl_normal;
CREATE TABLE t_ttl_normal (day Date, value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ttl_normal VALUES ('2000-01-01', 100);
ALTER TABLE t_ttl_normal MODIFY TTL day + INTERVAL 1 DAY;
OPTIMIZE TABLE t_ttl_normal FINAL;
SELECT count() FROM t_ttl_normal;
DROP TABLE t_ttl_normal;
