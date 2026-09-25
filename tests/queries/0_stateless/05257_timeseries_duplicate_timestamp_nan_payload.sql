-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A Prometheus stale marker (the NaN 0x7ff0000000000002) and an ordinary NaN share a timestamp.
-- Of two NaNs the greater bit pattern wins, so the ordinary NaN is kept whatever the order of the samples.

SET enable_time_series_table = 1;
SET enable_time_series_aggregate_functions = 1;

SELECT 'timeSeriesLastToGrid';
SELECT arrayMap(x -> hex(reinterpretAsUInt64(assumeNotNull(x))), timeSeriesLastToGrid(100, 100, 1, 10)([95, 95]::Array(UInt32), [reinterpretAsFloat64(0x7ff0000000000002), nan]));
SELECT arrayMap(x -> hex(reinterpretAsUInt64(assumeNotNull(x))), timeSeriesLastToGrid(100, 100, 1, 10)([95, 95]::Array(UInt32), [nan, reinterpretAsFloat64(0x7ff0000000000002)]));

SELECT 'timeSeriesFirstToGrid';
SELECT arrayMap(x -> hex(reinterpretAsUInt64(assumeNotNull(x))), timeSeriesFirstToGrid(100, 100, 1, 10)([95, 95]::Array(UInt32), [reinterpretAsFloat64(0x7ff0000000000002), nan]));
SELECT arrayMap(x -> hex(reinterpretAsUInt64(assumeNotNull(x))), timeSeriesFirstToGrid(100, 100, 1, 10)([95, 95]::Array(UInt32), [nan, reinterpretAsFloat64(0x7ff0000000000002)]));

SELECT 'timeSeriesMaxToGrid';
SELECT arrayMap(x -> hex(reinterpretAsUInt64(assumeNotNull(x))), timeSeriesMaxToGrid(100, 100, 1, 10)([95, 95]::Array(UInt32), [reinterpretAsFloat64(0x7ff0000000000002), nan]));
SELECT arrayMap(x -> hex(reinterpretAsUInt64(assumeNotNull(x))), timeSeriesMaxToGrid(100, 100, 1, 10)([95, 95]::Array(UInt32), [nan, reinterpretAsFloat64(0x7ff0000000000002)]));

SELECT 'timeSeriesGroupArray';
SELECT arrayMap(x -> (x.1, hex(reinterpretAsUInt64(x.2))), timeSeriesGroupArray([95, 95]::Array(UInt32), [reinterpretAsFloat64(0x7ff0000000000002), nan]));
SELECT arrayMap(x -> (x.1, hex(reinterpretAsUInt64(x.2))), timeSeriesGroupArray([95, 95]::Array(UInt32), [nan, reinterpretAsFloat64(0x7ff0000000000002)]));

SELECT 'Float32: two NaN payloads, the greater bit pattern wins';
SELECT arrayMap(x -> hex(reinterpretAsUInt32(assumeNotNull(x))), timeSeriesLastToGrid(100, 100, 1, 10)([95, 95]::Array(UInt32), [reinterpretAsFloat32(toUInt32(0x7fc00001)), toFloat32(nan)]));
SELECT arrayMap(x -> hex(reinterpretAsUInt32(assumeNotNull(x))), timeSeriesLastToGrid(100, 100, 1, 10)([95, 95]::Array(UInt32), [toFloat32(nan), reinterpretAsFloat32(toUInt32(0x7fc00001))]));

SELECT 'PromQL instant selector: the series is present with NaN';
DROP TABLE IF EXISTS ts_marker_first;
DROP TABLE IF EXISTS ts_nan_first;
CREATE TABLE ts_marker_first ENGINE = TimeSeries;
CREATE TABLE ts_nan_first ENGINE = TimeSeries;
INSERT INTO ts_marker_first (metric_name, tags, samples) VALUES
    ('m', map(), [(toDateTime64(95, 3), reinterpretAsFloat64(0x7ff0000000000002)), (toDateTime64(95, 3), nan)]);
INSERT INTO ts_nan_first (metric_name, tags, samples) VALUES
    ('m', map(), [(toDateTime64(95, 3), nan), (toDateTime64(95, 3), reinterpretAsFloat64(0x7ff0000000000002))]);
SELECT count(), countIf(isNaN(value)) FROM prometheusQuery(ts_marker_first, 'm', 100);
SELECT count(), countIf(isNaN(value)) FROM prometheusQuery(ts_nan_first, 'm', 100);
DROP TABLE ts_marker_first;
DROP TABLE ts_nan_first;
