-- Tags: no-fasttest
-- ANTLR4 is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS signed_at_u32;
DROP TABLE IF EXISTS signed_at_datetime;
DROP TABLE IF EXISTS signed_at_datetime64;

CREATE TABLE signed_at_u32 (samples Array(Tuple(UInt32, Float64))) ENGINE = TimeSeries;
CREATE TABLE signed_at_datetime (samples Array(Tuple(DateTime('UTC'), Float64))) ENGINE = TimeSeries;
CREATE TABLE signed_at_datetime64 (samples Array(Tuple(DateTime64(3, 'UTC'), Float64))) ENGINE = TimeSeries;

-- Subqueries without selectors must not wrap negative timestamps to unsigned values.
SELECT count() FROM prometheusQuery(signed_at_u32, 'vector(1)[5m:1m] @ -100', 0);
SELECT count() FROM prometheusQuery(signed_at_u32, 'vector(time())[5m:1m] @ -100', 0);
SELECT count() FROM prometheusQuery(signed_at_datetime, 'vector(1)[5m:1m] @ -100', 0);
SELECT count() FROM prometheusQuery(signed_at_datetime, 'vector(time())[5m:1m] @ -100', 0);

-- Clipping keeps the grid aligned and retains its nonnegative samples.
SELECT arrayMap(x -> toUInt32(x.1), samples) FROM prometheusQuery(signed_at_u32, 'vector(1)[5m:1m] @ +100', 0);
SELECT arrayMap(x -> x.2, samples) FROM prometheusQuery(signed_at_u32, 'vector(time())[5m:1m] @ +100', 0);
SELECT arrayMap(x -> toUInt32(x.1), samples) FROM prometheusQuery(signed_at_datetime, 'vector(1)[5m:1m] @ +100', 0);
SELECT arrayMap(x -> x.2, samples) FROM prometheusQuery(signed_at_datetime, 'vector(time())[5m:1m] @ +100', 0);

-- DateTime64 retains pre-epoch samples and rounds negative grid boundaries down.
SELECT arrayMap(x -> toUnixTimestamp64Milli(x.1), samples) FROM prometheusQuery(signed_at_datetime64, 'vector(1)[5m:1m] @ -100', 0);
SELECT arrayMap(x -> x.2, samples) FROM prometheusQuery(signed_at_datetime64, 'vector(time())[5m:1m] @ -100', 0);
SELECT arrayMap(x -> x.2, samples) FROM prometheusQuery(signed_at_datetime64, 'vector(time())[5m:1m] @ +100', 0);

DROP TABLE signed_at_u32;
DROP TABLE signed_at_datetime;
DROP TABLE signed_at_datetime64;
