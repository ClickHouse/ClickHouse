DROP TABLE IF EXISTS nnd;
CREATE TABLE nnd (id Int8, ts DateTime64(3, 'UTC'), metric Float64) ENGINE=MergeTree() ORDER BY id;
INSERT INTO nnd VALUES (1, toDateTime64('1979-12-12 21:21:21.123', 3, 'UTC'), 1.1), (2, toDateTime64('1979-12-12 21:21:21.124', 3, 'UTC'), 2.34), (3, toDateTime64('1979-12-12 21:21:21.127', 3, 'UTC'), 3.7), (4, toDateTime64('1979-12-12 21:21:21.129', 3, 'UTC'), 2.1), (5, toDateTime('1979-12-12 21:21:22', 'UTC'), 1.3345), (6, toDateTime('1979-12-12 21:21:23', 'UTC'), 1.54);

SELECT '- shall work for precise intervals';
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 3 NANOSECOND) FROM nnd;
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 4 MICROSECOND) FROM nnd;
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 5 MILLISECOND) FROM nnd;
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 6 SECOND) FROM nnd;
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 7 MINUTE) FROM nnd;
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 8 HOUR) FROM nnd;
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 9 DAY) FROM nnd;
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 10 WEEK) FROM nnd;

SELECT '- shall not work for month, quarter, year';
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 11 MONTH) FROM nnd; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 12 QUARTER) FROM nnd; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT ts, metric, nonNegativeDerivative(metric, ts, INTERVAL 13 YEAR) FROM nnd; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE IF EXISTS nnd;
