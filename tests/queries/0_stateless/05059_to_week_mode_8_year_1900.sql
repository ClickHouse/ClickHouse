-- `toWeek`/`toYearWeek` with mode 8 ("Sunday, 1-53, contains January 1") returned week 135 for most of the
-- year 1900: the Sunday that starts its first week is 1899-12-31, before the beginning of the date lookup
-- table, and rounding down a LUT index to that Sunday underflowed.

SELECT toWeek(toDate32('1900-06-15'), 8), toYearWeek(toDate32('1900-06-15'), 8);
SELECT toWeek(toDateTime64('1900-06-15 12:00:00', 3, 'UTC'), 8), toYearWeek(toDateTime64('1900-06-15 12:00:00', 3, 'UTC'), 8);
SELECT toWeek(toDate32('1900-01-01'), 8), toWeek(toDate32('1900-01-07'), 8), toWeek(toDate32('1900-12-29'), 8), toWeek(toDate32('1900-12-30'), 8);

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
           toYearWeek(toDate32(number - 25567), 8) % 100 AS year_a, toYearWeek(toDate32(number - 25567 + 146097), 8) % 100 AS year_b
    FROM numbers(365)
)
WHERE a != b OR year_a != year_b;

-- `toYearWeek` claims to be monotonic, so the wrong values made the primary key analysis prune granules
-- holding matching rows.
DROP TABLE IF EXISTS t_year_week_1900;
CREATE TABLE t_year_week_1900 (d Date32) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_year_week_1900 VALUES ('1900-01-01'), ('1900-06-15'), ('1900-06-16'), ('1900-06-17'), ('1901-06-01');

SELECT count() FROM t_year_week_1900 WHERE toYearWeek(d, 8) = 190024;
SELECT countIf(toYearWeek(d, 8) = 190024) FROM t_year_week_1900;

DROP TABLE t_year_week_1900;
