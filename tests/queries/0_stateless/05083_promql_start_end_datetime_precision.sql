-- Tags: no-fasttest
-- ANTLR4 is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS start_end_f32;
CREATE TABLE start_end_f32 (samples Array(Tuple(DateTime64(3, 'UTC'), Float32))) ENGINE = TimeSeries;

-- Float32 rounds this timestamp into the next minute, day, month, and year.
SELECT value FROM prometheusQuery(start_end_f32, 'minute(vector(start()))', 1735689599.125);
SELECT value FROM prometheusQuery(start_end_f32, 'minute(vector(end()))', 1735689599.125);
SELECT value FROM prometheusQuery(start_end_f32, 'year(vector(start()))', 1735689599.125);
SELECT value FROM prometheusQuery(start_end_f32, 'year(vector(end()))', 1735689599.125);
SELECT value FROM prometheusQuery(start_end_f32, 'minute(vector(+scalar(vector(start()))))', 1735689599.125);
SELECT value FROM prometheusQuery(start_end_f32, 'minute(vector(+scalar(vector(end()))))', 1735689599.125);

-- Existing native-precision time() handling is unchanged.
SELECT value FROM prometheusQuery(start_end_f32, 'minute(vector(time()))', 1735689599.125);
SELECT value FROM prometheusQuery(start_end_f32, 'minute()', 1735689599.125);

-- The requested end is not on the evaluation grid and differs from the start.
SELECT arrayMap(x -> x.2, samples) FROM prometheusQueryRange(start_end_f32, 'minute(vector(start()))', 1735689599.125, 1735689661.375, 30);
SELECT arrayMap(x -> x.2, samples) FROM prometheusQueryRange(start_end_f32, 'minute(vector(end()))', 1735689599.125, 1735689661.375, 30);
SELECT arrayMap(x -> x.2, samples) FROM prometheusQueryRange(start_end_f32, 'year(vector(start()))', 1735689599.125, 1735689661.375, 30);
SELECT arrayMap(x -> x.2, samples) FROM prometheusQueryRange(start_end_f32, 'year(vector(end()))', 1735689599.125, 1735689661.375, 30);

-- Subqueries keep the outer query's timestamp, not their shifted evaluation time.
SELECT value FROM prometheusQuery(start_end_f32, 'last_over_time(minute(vector(start()))[20s:5s] offset 7s)', 1735689599.125);
SELECT value FROM prometheusQuery(start_end_f32, 'last_over_time(minute(vector(end()))[20s:5s] @ 1005)', 1735689599.125);

DROP TABLE start_end_f32;
