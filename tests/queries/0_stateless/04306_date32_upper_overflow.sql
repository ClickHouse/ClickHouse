-- Upper-range counterpart of 04241_date32_week_overflow.
-- `Date32`/`DateTime64` values whose rounded result falls after the `Date` upper bound
-- (2149-06-06, day number 0xFFFF) must clamp to 2149-06-06 instead of wrapping through UInt16.

SET session_timezone = 'UTC';

SELECT toStartOfWeek(toDate32('2299-12-31'));
SELECT toLastDayOfWeek(toDate32('2299-12-31'));
SELECT toMonday(toDate32('2299-12-31'));
SELECT toStartOfMonth(toDate32('2299-12-31'));
SELECT toLastDayOfMonth(toDate32('2299-12-31'));
SELECT toStartOfQuarter(toDate32('2299-12-31'));
SELECT toStartOfYear(toDate32('2299-12-31'));
SELECT toMonday(toDateTime64('2299-12-31 00:00:00', 0));

-- Sanity: results that fit into Date are not clamped.
SELECT toStartOfMonth(toDate32('2149-06-06'));
SELECT toStartOfYear(toDate32('2149-06-06'));

-- Wrong-result counterpart of https://github.com/ClickHouse/ClickHouse/issues/106419 in the
-- upper range: without the clamp, `toStartOfYear` of a post-2149 `Date32` wraps to a small
-- `Date`, the part's value range becomes inverted, and a `WHERE` filter using the monotonic
-- function analysis prunes parts that contain matching rows. `countIf` (per-row evaluation)
-- is the ground truth and must match.
DROP TABLE IF EXISTS t_date32_upper;
CREATE TABLE t_date32_upper (c1 Date32) ENGINE = MergeTree ORDER BY tuple();
-- one row per year: 2140 .. 2299
INSERT INTO t_date32_upper SELECT toDate32(concat(toString(2140 + number), '-07-15')) FROM numbers(160);
OPTIMIZE TABLE t_date32_upper FINAL;

SELECT countIf(toStartOfYear(c1) >= toDate('2100-01-01')) FROM t_date32_upper;
SELECT count() FROM t_date32_upper WHERE toStartOfYear(c1) >= toDate('2100-01-01');
SELECT countIf(toStartOfYear(c1) < toDate('1980-01-01')) FROM t_date32_upper;
SELECT count() FROM t_date32_upper WHERE toStartOfYear(c1) < toDate('1980-01-01');

DROP TABLE t_date32_upper;
