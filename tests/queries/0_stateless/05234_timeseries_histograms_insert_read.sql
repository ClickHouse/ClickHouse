-- The `histograms.*` outer columns of the TimeSeries table engine: inserting histogram samples through them stores rows in the
-- inner histograms table, and reading them returns the samples back (see TimeSeriesVersion.h, version 6).

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_hist;
DROP TABLE IF EXISTS ts_v5;

CREATE TABLE ts_hist ENGINE = TimeSeries;

SELECT '--- the outer columns of the histograms group ---';
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'ts_hist' AND name LIKE 'histograms.%' ORDER BY position;

SELECT '--- inserting histograms of every flavour ---';
-- An integer histogram with exponential buckets: 4 positive buckets with the indexes -2, -1, 1, 2 and one negative bucket.
INSERT INTO ts_hist (metric_name, tags, histograms.timestamp, histograms.schema, histograms.zero_threshold, histograms.count_int, histograms.zero_count_int, histograms.sum,
                     histograms.positive_spans, histograms.positive_values_int, histograms.negative_spans, histograms.negative_values_int)
    VALUES ('latency', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [3], [0.001], [50], [2], [12.5], [[(-2, 2), (1, 2)]], [[3, 10, 20, 9]], [[(0, 1)]], [[6]]);

-- A float histogram of a gauge (counter_reset_hint = 3).
INSERT INTO ts_hist (metric_name, tags, histograms.timestamp, histograms.is_float, histograms.counter_reset_hint, histograms.schema, histograms.zero_threshold,
                     histograms.count_float, histograms.zero_count_float, histograms.sum, histograms.positive_spans, histograms.positive_values_float)
    VALUES ('latency_float', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [true], [3], [2], [0.001], [7.5], [0.5], [3.25], [[(0, 3)]], [[1.5, 2.5, 3]]);

-- An integer histogram with custom buckets: 3 bounds define 4 buckets, the last one up to +Inf.
INSERT INTO ts_hist (metric_name, tags, histograms.timestamp, histograms.schema, histograms.count_int, histograms.sum,
                     histograms.positive_spans, histograms.positive_values_int, histograms.custom_values)
    VALUES ('latency_nhcb', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [-53], [15], [4.2], [[(0, 4)]], [[1, 2, 4, 8]], [[0.1, 0.5, 1]]);

-- A row with a float sample and two histogram samples of the same time series.
INSERT INTO ts_hist (metric_name, tags, samples, histograms.timestamp, histograms.schema, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('mixed', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.5)],
            [toDateTime64('2026-01-01 00:00:10', 3), toDateTime64('2026-01-01 00:00:20', 3)], [3, 3], [1, 2], [0.5, 1.5], [[(0, 1)], [(0, 1)]], [[1], [2]]);

-- A float-only time series, a metadata-only row, and an insert without a column list.
INSERT INTO ts_hist (metric_name, tags, samples) VALUES ('up', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1)]);
INSERT INTO ts_hist (metric_family, type, unit, help) VALUES ('latency', 'histogram', 'seconds', 'Request latency');
INSERT INTO ts_hist VALUES ('up', {'job': 'web'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1)], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], '', '', '', '');

SELECT count() FROM ts_hist;
SELECT count() FROM timeSeriesHistograms(ts_hist);

SELECT '--- the rows of the histograms table ---';
SELECT t.metric_name, h.timestamp, h.is_float, h.counter_reset_hint, h.schema, h.zero_threshold, h.sum, h.positive_spans, h.negative_spans, h.custom_values,
       h.count_int, h.zero_count_int, h.positive_values_int, h.negative_values_int, h.count_float, h.zero_count_float, h.positive_values_float, h.negative_values_float
    FROM timeSeriesHistograms(ts_hist) AS h
    INNER JOIN timeSeriesTags(ts_hist) AS t ON h.id = t.id
    ORDER BY t.metric_name, h.timestamp;

SELECT '--- the time range of a time series covers its histogram samples ---';
SELECT metric_name, min_time, max_time FROM timeSeriesTags(ts_hist) ORDER BY metric_name, tags['job'];

SELECT '--- reading the outer columns: a time series with samples of one kind only gets empty arrays for the other kind ---';
SELECT metric_name, tags, samples, histograms.timestamp, histograms.schema, histograms.count_int, histograms.count_float FROM ts_hist ORDER BY metric_name, tags['job'];

SELECT '--- reading the data columns only ---';
SELECT samples, histograms.count_int FROM ts_hist ORDER BY samples, histograms.count_int;
SELECT histograms.timestamp, histograms.sum FROM ts_hist ORDER BY histograms.timestamp, histograms.sum;

SELECT '--- ARRAY JOIN over the group ---';
SELECT metric_name, t, counts FROM ts_hist ARRAY JOIN histograms.timestamp AS t, histograms.positive_values_int AS counts ORDER BY metric_name, t;

SELECT '--- errors ---';
-- The other columns of the group can be omitted, but not the timestamp.
INSERT INTO ts_hist (metric_name, tags, histograms.count_int) VALUES ('x', {'job': 'api'}, [1]); -- { serverError ILLEGAL_COLUMN }
-- The arrays of the group must have the same length in a row.
INSERT INTO ts_hist (metric_name, tags, histograms.timestamp, histograms.count_int) VALUES ('x', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)], [1, 2]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
-- A histogram sample needs a time series.
INSERT INTO ts_hist (histograms.timestamp) VALUES ([toDateTime64('2026-01-01 00:00:00', 3)]); -- { serverError INCORRECT_DATA }
SELECT count() FROM timeSeriesHistograms(ts_hist);

SELECT '--- a table of version 5 has no histograms columns ---';
CREATE TABLE ts_v5 ENGINE = TimeSeries SETTINGS version = 5;
SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'ts_v5' AND name LIKE 'histograms.%';
INSERT INTO ts_v5 (metric_name, tags, histograms.timestamp) VALUES ('x', {'job': 'api'}, [toDateTime64('2026-01-01 00:00:00', 3)]); -- { serverError NO_SUCH_COLUMN_IN_TABLE }

DROP TABLE ts_v5;
DROP TABLE ts_hist;
