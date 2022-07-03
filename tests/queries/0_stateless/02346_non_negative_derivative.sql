DROP TABLE IF EXISTS nnd;

CREATE TABLE nnd
(
    id Int8, ts DateTime64(3, 'UTC'), metric Float64
)
ENGINE=MergeTree()
ORDER BY id;

INSERT INTO nnd VALUES (1, toDateTime64('1979-12-12 21:21:21.123', 3, 'UTC'), 1.1), (2, toDateTime64('1979-12-12 21:21:21.124', 3, 'UTC'), 2.34), (3, toDateTime64('1979-12-12 21:21:21.127', 3, 'UTC'), 3.7);
INSERT INTO nnd VALUES (4, toDateTime64('1979-12-12 21:21:21.129', 3, 'UTC'), 2.1), (5, toDateTime('1979-12-12 21:21:22', 'UTC'), 1.3345), (6, toDateTime('1979-12-12 21:21:23', 'UTC'), 1.54), (7, toDateTime('1979-12-12 21:21:23', 'UTC'), 1.54);

-- shall work for precise intervals
-- INTERVAL 1 SECOND shall be default
SELECT (
           SELECT
               ts,
               metric,
               nonNegativeDerivative(metric, ts) OVER (PARTITION BY metric ORDER BY ts ASC Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS deriv
           FROM nnd
           LIMIT 5, 1
       ) = (
           SELECT
               ts,
               metric,
               nonNegativeDerivative(metric, ts, toIntervalSecond(1)) OVER (PARTITION BY metric ORDER BY ts ASC Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS deriv
           FROM nnd
           LIMIT 5, 1
       );
SELECT ts, metric, nonNegativeDerivative(metric, ts) OVER (PARTITION BY metric ORDER BY ts ASC Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS deriv FROM nnd;
-- Nanosecond
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 3 NANOSECOND) OVER (PARTITION BY metric ORDER BY ts ASC Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS deriv FROM nnd;
-- Microsecond
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 4 MICROSECOND) OVER (PARTITION BY metric ORDER BY ts ASC Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS deriv FROM nnd;
-- Millisecond
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 5 MILLISECOND) OVER (PARTITION BY metric ORDER BY ts ASC Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS deriv FROM nnd;
-- Second
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 6 SECOND) OVER (PARTITION BY metric ORDER BY ts ASC Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS deriv FROM nnd;
-- Minute
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 7 MINUTE) OVER (PARTITION BY metric ORDER BY ts ASC Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS deriv FROM nnd;
-- Hour
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 8 HOUR) OVER (ORDER BY ts ASC Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS deriv FROM nnd;
-- Day
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 9 DAY) OVER (ORDER BY ts ASC Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS deriv FROM nnd;
-- Week
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 10 WEEK) OVER (ORDER BY ts ASC Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS deriv FROM nnd;

-- shall not work for month, quarter, year (intervals with floating number of seconds)
-- Month
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 11 MONTH) OVER (PARTITION BY metric ORDER BY ts ASC Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS deriv FROM nnd; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- Quarter
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 12 QUARTER) OVER (PARTITION BY metric ORDER BY ts ASC Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS deriv FROM nnd; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- Year
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 13 YEAR) OVER (PARTITION BY metric ORDER BY ts ASC Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS deriv FROM nnd; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- test against wrong arguments/types
SELECT ts, metric, nonNegativeDerivative(metric, 1, INTERVAL 3 NANOSECOND) OVER (PARTITION BY metric ORDER BY ts ASC Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS deriv FROM nnd; -- { serverError BAD_ARGUMENTS }
SELECT ts, metric, nonNegativeDerivative('string not datetime', ts, INTERVAL 3 NANOSECOND) OVER (PARTITION BY metric ORDER BY ts ASC Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS deriv FROM nnd; -- { serverError BAD_ARGUMENTS }
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 3 NANOSECOND, id) OVER (PARTITION BY metric ORDER BY ts ASC Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS deriv FROM nnd; -- { serverError BAD_ARGUMENTS }
SELECT ts, metric, nonNegativeDerivative(metric) OVER (PARTITION BY metric ORDER BY ts ASC Rows BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS deriv FROM nnd; -- { serverError BAD_ARGUMENTS }

-- cleanup
DROP TABLE IF EXISTS nnd;
