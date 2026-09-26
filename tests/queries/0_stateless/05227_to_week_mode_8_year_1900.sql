-- `toWeek`/`toYearWeek` with mode 8 ("Sunday, 1-53, contains January 1") returned week 135 for most of the
-- year 1900: the Sunday that starts its first week is 1899-12-31, before the beginning of the date lookup
-- table, and rounding down a LUT index to that Sunday underflowed.

SELECT toWeek(toDate32('1900-06-15'), 8), toYearWeek(toDate32('1900-06-15'), 8);
SELECT toWeek(toDateTime64('1900-06-15 12:00:00', 3, 'UTC'), 8), toYearWeek(toDateTime64('1900-06-15 12:00:00', 3, 'UTC'), 8);
SELECT toWeek(toDate32('1900-01-01'), 8), toWeek(toDate32('1900-01-07'), 8), toWeek(toDate32('1900-12-29'), 8), toWeek(toDate32('1900-12-30'), 8);

-- The week of the last day covered by the date lookup table, 2299-12-31, ends at 2300-01-06 and therefore
-- contains January 1 - the week-year has to advance past the end of the table, and so does its 1899-12-31
-- counterpart from the previous 400 year cycle. Mode 6 agrees, and mode 9 does not, because its Monday-first
-- week ends before January 1.
SELECT toYearWeek(toDate32('2299-12-31'), 8), toWeek(toDate32('2299-12-31'), 8), toYearWeek(toDate32('2299-12-31'), 6), toYearWeek(toDate32('2299-12-31'), 9);
SELECT toYearWeek(toDate32('1899-12-31'), 8), toWeek(toDate32('1899-12-31'), 8), toYearWeek(toDate32('1899-12-31'), 9);

-- Every mode stays inside the documented range over a whole 400 year cycle starting at 1900-01-01.
SELECT mode, min(w), max(w)
FROM
(
    SELECT 8 AS mode, toWeek(toDate32(number - 25567), 8) AS w FROM numbers(146097)
    UNION ALL
    SELECT 9, toWeek(toDate32(number - 25567), 9) FROM numbers(146097)
)
GROUP BY mode ORDER BY mode;

-- Week numbering repeats every 400 years, so 1900 and 2300 must agree.
SELECT count()
FROM
(
    SELECT toWeek(toDate32(number - 25567), 8) AS a, toWeek(toDate32(number - 25567 + 146097), 8) AS b,
           toYearWeek(toDate32(number - 25567), 8) + 40000 AS year_week_a, toYearWeek(toDate32(number - 25567 + 146097), 8) AS year_week_b
    FROM numbers(146097)
)
WHERE a != b OR year_week_a != year_week_b;

-- `toYearWeek` claims to be monotonic, so the wrong values made the primary key analysis prune granules
-- holding matching rows.
DROP TABLE IF EXISTS t_year_week_1900;
CREATE TABLE t_year_week_1900 (d Date32) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_year_week_1900 VALUES ('1900-01-01'), ('1900-06-15'), ('1900-06-16'), ('1900-06-17'), ('1901-06-01');

SELECT count() FROM t_year_week_1900 WHERE toYearWeek(d, 8) = 190024;
SELECT countIf(toYearWeek(d, 8) = 190024) FROM t_year_week_1900;

DROP TABLE t_year_week_1900;

-- The same holds at the ends of the whole representable range, which lie outside of the lookup table and
-- are computed by shifting through the 400 year cycle. The Sunday-first week of 9999-12-26 and the
-- Monday-first week of 9999-12-27 contain January 1 of the year 10000, so their week-year is 10000 and
-- must not be clamped back to 9999, which would make `toYearWeek` fall from 999952 to 999901. 0000-01-01 is
-- a Saturday belonging to the week-year -1, which is not representable: `toYearWeek` saturates to zero - the
-- value sorting before every other one - so that it does not fall on the next day either. The saturation is
-- confined to the `YYYYWW` number: `toWeek` keeps reporting the real week number of such a day.
SELECT d, toYearWeek(d, 8), toYearWeek(d, 9), toYearWeek(d, 0), toYearWeek(d, 3), toWeek(d, 8), toWeek(d, 9), toWeek(d, 0), toWeek(d, 3)
FROM (SELECT toDate32('9999-12-25') + number AS d FROM numbers(3) UNION ALL SELECT toDate32('0000-01-01') + number FROM numbers(3))
ORDER BY d;

-- `toWeek(date, 3)` is documented to be exactly `toISOWeek(date)` and to return a number in the `1-53`
-- range, including for the days at the bottom of the range, whose ISO week belongs to the unrepresentable
-- week-year -1.
SELECT d, toWeek(d, 3), toISOWeek(d), toWeek(d, 1), toWeek(d, 5), toWeek(d, 7)
FROM (SELECT toDate32('0000-01-01') + number AS d FROM numbers(4))
ORDER BY d;

SELECT count()
FROM (SELECT toDate32('0000-01-01') + number AS d FROM numbers(1000) UNION ALL SELECT toDate32('9999-12-31') - number FROM numbers(1000))
WHERE toWeek(d, 3) != toISOWeek(d) OR toWeek(d, 3) < 1 OR toWeek(d, 3) > 53;

-- `toYearWeek` must be non-decreasing over the whole representable range for every mode.
SELECT mode, count()
FROM
(
    SELECT mode, w, any(w) OVER (PARTITION BY mode ORDER BY d ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS prev_w
    FROM
    (
    SELECT 0 AS mode, d, toYearWeek(d, 0) AS w FROM (SELECT toDate32('9999-12-01') + number AS d FROM numbers(31) UNION ALL SELECT toDate32('0000-01-01') + number FROM numbers(31))
    UNION ALL
    SELECT 1 AS mode, d, toYearWeek(d, 1) AS w FROM (SELECT toDate32('9999-12-01') + number AS d FROM numbers(31) UNION ALL SELECT toDate32('0000-01-01') + number FROM numbers(31))
    UNION ALL
    SELECT 2 AS mode, d, toYearWeek(d, 2) AS w FROM (SELECT toDate32('9999-12-01') + number AS d FROM numbers(31) UNION ALL SELECT toDate32('0000-01-01') + number FROM numbers(31))
    UNION ALL
    SELECT 3 AS mode, d, toYearWeek(d, 3) AS w FROM (SELECT toDate32('9999-12-01') + number AS d FROM numbers(31) UNION ALL SELECT toDate32('0000-01-01') + number FROM numbers(31))
    UNION ALL
    SELECT 4 AS mode, d, toYearWeek(d, 4) AS w FROM (SELECT toDate32('9999-12-01') + number AS d FROM numbers(31) UNION ALL SELECT toDate32('0000-01-01') + number FROM numbers(31))
    UNION ALL
    SELECT 5 AS mode, d, toYearWeek(d, 5) AS w FROM (SELECT toDate32('9999-12-01') + number AS d FROM numbers(31) UNION ALL SELECT toDate32('0000-01-01') + number FROM numbers(31))
    UNION ALL
    SELECT 6 AS mode, d, toYearWeek(d, 6) AS w FROM (SELECT toDate32('9999-12-01') + number AS d FROM numbers(31) UNION ALL SELECT toDate32('0000-01-01') + number FROM numbers(31))
    UNION ALL
    SELECT 7 AS mode, d, toYearWeek(d, 7) AS w FROM (SELECT toDate32('9999-12-01') + number AS d FROM numbers(31) UNION ALL SELECT toDate32('0000-01-01') + number FROM numbers(31))
    UNION ALL
    SELECT 8 AS mode, d, toYearWeek(d, 8) AS w FROM (SELECT toDate32('9999-12-01') + number AS d FROM numbers(31) UNION ALL SELECT toDate32('0000-01-01') + number FROM numbers(31))
    UNION ALL
    SELECT 9 AS mode, d, toYearWeek(d, 9) AS w FROM (SELECT toDate32('9999-12-01') + number AS d FROM numbers(31) UNION ALL SELECT toDate32('0000-01-01') + number FROM numbers(31))
    )
)
WHERE w < prev_w
GROUP BY mode ORDER BY mode;

DROP TABLE IF EXISTS t_year_week_9999;
CREATE TABLE t_year_week_9999 (d Date32) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_year_week_9999 VALUES ('0000-01-01'), ('0000-01-02'), ('9999-12-24'), ('9999-12-25'), ('9999-12-26'), ('9999-12-27'), ('9999-12-31');

SELECT count() FROM t_year_week_9999 WHERE toYearWeek(d, 8) = 1000001;
SELECT countIf(toYearWeek(d, 8) = 1000001) FROM t_year_week_9999;
SELECT count() FROM t_year_week_9999 WHERE toYearWeek(d, 9) = 999952;
SELECT countIf(toYearWeek(d, 9) = 999952) FROM t_year_week_9999;
SELECT count() FROM t_year_week_9999 WHERE toYearWeek(d, 0) = 1;
SELECT countIf(toYearWeek(d, 0) = 1) FROM t_year_week_9999;

DROP TABLE t_year_week_9999;

-- A `Date32` column is just an `Int32`, so it can hold day numbers beyond 9999-12-31 (arithmetic does not
-- saturate the stored value, only the formatting does). They must saturate to the boundary day like in
-- every other calendar function, instead of letting the week-year run away past the four-digit year and
-- eventually wrap around: the week-year of the last representable day is 10000 in mode 8, not 10400.
SELECT toYear(d), toYearWeek(d, 8), toYearWeek(d, 9), toYearWeek(d, 0), toWeek(d, 8)
FROM (SELECT toDate32('9999-12-31') + 146097 * number AS d FROM numbers(1, 3))
ORDER BY d;

SELECT toYear(d), toYearWeek(d, 8), toYearWeek(d, 9), toYearWeek(d, 0), toWeek(d, 8)
FROM (SELECT toDate32('0000-01-01') - 146097 * number AS d FROM numbers(1, 3))
ORDER BY d;
