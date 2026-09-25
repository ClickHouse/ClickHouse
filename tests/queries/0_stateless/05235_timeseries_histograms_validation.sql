-- The validation of histogram samples inserted into a TimeSeries table (see validateTimeSeriesHistograms.h):
-- the rules follow Prometheus's `Histogram.Validate` and `FloatHistogram.Validate`. An insert with an invalid histogram is rejected.
-- The columns of the `histograms` group which an INSERT doesn't mention get default values, so a valid histogram needs only a timestamp.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_val;
DROP TABLE IF EXISTS ts_val_limited;

CREATE TABLE ts_val ENGINE = TimeSeries;

SELECT '--- counter_reset_hint is 0..3 ---';
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.counter_reset_hint) VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [4]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.counter_reset_hint) VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [3]);

SELECT '--- schema is -4..8 or -53 ---';
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema) VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [9]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema) VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [-5]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema) VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [-52]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema) VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [-4]);
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema) VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [8]);
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema) VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [-53]);

SELECT '--- exponential buckets: the spans describe the buckets and go forwards after the first span ---';
-- A span after the first has a negative offset.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [2], [[(0, 1), (-1, 1)]], [[1, 1]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.negative_spans, histograms.negative_values_int)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [2], [[(0, 1), (-1, 1)]], [[1, 1]]); -- { serverError INCORRECT_DATA }
-- The first span may have a negative offset.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [2], [[(-5, 2)]], [[1, 1]]);
-- The lengths of the spans don't sum up to the number of buckets.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [1], [[(0, 2)]], [[1]]); -- { serverError INCORRECT_DATA }
-- The bucket indexes overflow Int32.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [2], [[(2147483647, 1), (1, 1)]], [[1, 1]]); -- { serverError INCORRECT_DATA }
-- Custom bounds belong to custom buckets only.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.custom_values) VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [[1]]); -- { serverError INCORRECT_DATA }

SELECT '--- float histograms: no negative counts, the total count is not checked ---';
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.is_float, histograms.positive_spans, histograms.positive_values_float)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [true], [[(0, 1)]], [[-1]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.is_float, histograms.zero_count_float)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [true], [-1]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.is_float, histograms.count_float, histograms.positive_spans, histograms.positive_values_float)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [true], [100], [[(0, 1)]], [[1]]);

SELECT '--- integer histograms: the buckets and the zero bucket sum up to the count, or to at most the count when the sum is NaN ---';
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [5], [[(0, 2)]], [[1, 2]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.zero_count_int, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [3], [1], [[(0, 1)]], [[2]]);
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [5], [nan], [[(0, 2)]], [[1, 2]]);
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [2], [nan], [[(0, 2)]], [[1, 2]]); -- { serverError INCORRECT_DATA }
-- The sum of the buckets overflows UInt64.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [0], [[(0, 2)]], [[18446744073709551615, 1]]); -- { serverError INCORRECT_DATA }

SELECT '--- custom buckets: increasing finite bounds covering the spans, no negative side, no zero bucket ---';
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.count_int, histograms.positive_spans, histograms.positive_values_int, histograms.custom_values)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [-53], [15], [[(0, 4)]], [[1, 2, 4, 8]], [[0.1, 0.5, 1]]);
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.custom_values)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [-53], [[nan]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.custom_values)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [-53], [[1, 1]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.custom_values)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [-53], [[2, 1]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.custom_values)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [-53], [[1, inf]]); -- { serverError INCORRECT_DATA }
-- Every span of custom buckets has a non-negative offset.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.count_int, histograms.positive_spans, histograms.positive_values_int, histograms.custom_values)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [-53], [1], [[(-1, 1)]], [[1]], [[1]]); -- { serverError INCORRECT_DATA }
-- One bound defines two buckets, but the spans need three.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.count_int, histograms.positive_spans, histograms.positive_values_int, histograms.custom_values)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [-53], [3], [[(0, 3)]], [[1, 1, 1]], [[0.5]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.count_int, histograms.negative_spans, histograms.negative_values_int)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [-53], [1], [[(0, 1)]], [[1]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.count_int, histograms.zero_count_int)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [-53], [1], [1]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.zero_threshold)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [-53], [0.001]); -- { serverError INCORRECT_DATA }

SELECT '--- the accepted histograms ---';
SELECT count() FROM timeSeriesHistograms(ts_val);

SELECT '--- the histograms_max_buckets setting ---';
CREATE TABLE ts_val_limited ENGINE = TimeSeries SETTINGS histograms_max_buckets = 2;
INSERT INTO ts_val_limited (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [2], [[(0, 2)]], [[1, 1]]);
INSERT INTO ts_val_limited (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.positive_spans, histograms.positive_values_int, histograms.negative_spans, histograms.negative_values_int)
    VALUES ('m', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [3], [[(0, 2)]], [[1, 1]], [[(0, 1)]], [[1]]); -- { serverError INCORRECT_DATA }
SELECT count() FROM timeSeriesHistograms(ts_val_limited);
CREATE TABLE ts_val_v5 ENGINE = TimeSeries SETTINGS version = 5, histograms_max_buckets = 2; -- { serverError INVALID_SETTING_VALUE }

DROP TABLE ts_val_limited;
DROP TABLE ts_val;
