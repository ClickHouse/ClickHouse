-- Tags: no-fasttest
-- ANTLR4 is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS signed_at_u32;
DROP TABLE IF EXISTS signed_at_datetime;
DROP TABLE IF EXISTS signed_at_datetime64;

CREATE TABLE signed_at_u32 (samples Array(Tuple(UInt32, Float64))) ENGINE = TimeSeries;
CREATE TABLE signed_at_datetime (samples Array(Tuple(DateTime('UTC'), Float64))) ENGINE = TimeSeries;
CREATE TABLE signed_at_datetime64 (samples Array(Tuple(DateTime64(3, 'UTC'), Float64))) ENGINE = TimeSeries;

-- Selector-free subqueries use the result DateTime64 grid, independent of the table timestamp type.
SELECT arrayMap(x -> toUnixTimestamp64Second(x.1), samples) FROM prometheusQuery(signed_at_u32, 'vector(1)[5m:1m] @ -100', 0);
SELECT arrayMap(x -> x.2, samples) FROM prometheusQuery(signed_at_u32, 'vector(time())[5m:1m] @ -100', 0);
SELECT arrayMap(x -> toUnixTimestamp64Second(x.1), samples) FROM prometheusQuery(signed_at_datetime, 'vector(1)[5m:1m] @ -100', 0);
SELECT arrayMap(x -> x.2, samples) FROM prometheusQuery(signed_at_datetime, 'vector(time())[5m:1m] @ -100', 0);
SELECT arrayMap(x -> toUnixTimestamp64Second(x.1), samples) FROM prometheusQuery(signed_at_datetime64, 'vector(1)[5m:1m] @ -100', 0);
SELECT arrayMap(x -> x.2, samples) FROM prometheusQuery(signed_at_datetime64, 'vector(time())[5m:1m] @ -100', 0);

-- Negative grid boundaries round down to the preceding step.
SELECT arrayMap(x -> x.2, samples) FROM prometheusQuery(signed_at_datetime64, 'vector(time())[5m:1m] @ +100', 0);

DROP TABLE signed_at_u32;
DROP TABLE signed_at_datetime;
DROP TABLE signed_at_datetime64;
