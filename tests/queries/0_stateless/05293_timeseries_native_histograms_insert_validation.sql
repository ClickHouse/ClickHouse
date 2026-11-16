-- Test: an INSERT into the outer `histograms` column of a TimeSeries table runs the same checks as the Prometheus
-- remote-write protocol, and a rejected INSERT leaves no rows in any target table.
-- The histogram tuple: (timestamp, flags, schema, zero_threshold, count, sum, zero_count, positive_spans, positive_values,
-- negative_spans, negative_values, custom_values, count_int, zero_count_int, positive_values_int, negative_values_int).

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_hist_validation;
DROP TABLE IF EXISTS ts_hist_partial;

CREATE TABLE ts_hist_validation ENGINE = TimeSeries SETTINGS store_native_histograms = 1;

SELECT '-- NaN counts are rejected unless the histogram is a stale marker';
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 1, 0, 0., nan, 1., 0., [], [], [], [], [], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 1, 0, 0., 1., 1., nan, [], [], [], [], [], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 1, 0, 0., 1., 1., 0., [(0, 1)], [nan], [], [], [], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 1, 0, 0., 1., 1., 0., [], [], [(0, 1)], [nan], [], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
-- The same NaN counts in a stale marker (flags: float 1 | stale marker 16) are accepted.
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 17, 0, 0., nan, nan, nan, [(0, 1)], [nan], [], [], [], 0, 0, [], [])]);

SELECT '-- custom buckets (schema -53) must not use the zero bucket and need finite strictly increasing bounds';
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(2, 3), 0, -53, 0., 2., 1., 1., [(0, 1)], [1.], [], [], [1.], 2, 1, [1], [])]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(2, 3), 1, -53, 0.5, 1., 1., 0., [(0, 1)], [1.], [], [], [1.], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(2, 3), 1, -53, 0., 1., 1., 0., [(0, 1)], [1.], [], [], [2., 1.], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(2, 3), 1, -53, 0., 1., 1., 0., [(0, 1)], [1.], [], [], [1., 1.], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(2, 3), 1, -53, 0., 1., 1., 0., [(0, 1)], [1.], [], [], [1., inf], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(2, 3), 1, -53, 0., 1., 1., 0., [(0, 1)], [1.], [], [], [nan], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(2, 3), 1, -53, 0., 1., 1., 0., [], [], [(0, 1)], [1.], [1.], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(2, 3), 1, -53, 0., 2., 1., 0., [(0, 3)], [1., 1., 0.], [], [], [1.], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
-- An exponential schema must not have custom bucket bounds.
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(2, 3), 1, 0, 0., 1., 1., 0., [(0, 1)], [1.], [], [], [1.], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
-- A valid custom-bucket histogram is accepted: bucket 2 is one past the last bound, its upper bound is +Inf.
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(2, 3), 1, -53, 0., 4., 4.5, 0., [(0, 3)], [2., 1., 1.], [], [], [1., 2.5], 0, 0, [], [])]);

SELECT '-- the exact integer counts must match the counts of an integer histogram and be empty for a float one';
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(3, 3), 1, 0, 0., 1., 1., 0., [(0, 1)], [1.], [], [], [], 1, 0, [1], [])]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(3, 3), 0, 0, 0., 2., 1., 0., [(0, 1)], [2.], [], [], [], 1, 0, [2], [])]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(3, 3), 0, 0, 0., 2., 1., 0., [(0, 1)], [2.], [], [], [], 2, 0, [3], [])]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(3, 3), 0, 0, 0., 2., 1., 0., [(0, 1)], [2.], [], [], [], 2, 0, [], [])]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(3, 3), 0, 0, 0., 2., 1., 0., [(0, 1)], [2.], [], [], [], 2, 0, [2], [2])]); -- { serverError INCORRECT_DATA }
-- A consistent integer histogram is accepted.
INSERT INTO ts_hist_validation (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(3, 3), 0, 0, 0., 2., 1., 0., [(0, 1)], [2.], [], [], [], 2, 0, [2], [])]);

SELECT '-- the accepted histograms';
SELECT timestamp, flags, `schema`, count, zero_count, custom_values, count_int, positive_values_int
FROM timeSeriesHistograms(ts_hist_validation) ORDER BY timestamp;

DROP TABLE ts_hist_validation;

CREATE TABLE ts_hist_partial ENGINE = TimeSeries SETTINGS store_native_histograms = 1;

SELECT '-- a rejected INSERT leaves no rows in the tags, samples and histograms tables';
-- The second series carries an invalid histogram (a NaN count).
INSERT INTO ts_hist_partial (metric_name, tags, samples, histograms) VALUES
    ('good', map('job', 'x'), [(toDateTime64(1, 3), 1.)], [(toDateTime64(1, 3), 1, 0, 0., 1., 1., 0., [], [], [], [], [], 0, 0, [], [])]),
    ('bad', map('job', 'y'), [(toDateTime64(1, 3), 2.)], [(toDateTime64(1, 3), 1, 0, 0., nan, 1., 0., [], [], [], [], [], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
-- The second row has a histogram but neither a metric name nor tags.
INSERT INTO ts_hist_partial (metric_name, tags, samples, histograms) VALUES
    ('good', map('job', 'x'), [(toDateTime64(1, 3), 1.)], []),
    ('', map(), [], [(toDateTime64(1, 3), 1, 0, 0., 1., 1., 0., [], [], [], [], [], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
SELECT
    (SELECT count() FROM timeSeriesTags(ts_hist_partial)),
    (SELECT count() FROM timeSeriesSamples(ts_hist_partial)),
    (SELECT count() FROM timeSeriesHistograms(ts_hist_partial));

DROP TABLE ts_hist_partial;
