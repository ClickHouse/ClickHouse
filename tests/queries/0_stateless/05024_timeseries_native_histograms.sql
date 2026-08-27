-- Tags: no-replicated-database
-- Reason: DETACH/ATTACH of a TimeSeries table hangs in DatabaseReplicated mode because the DDL
-- goes through the replicated log and requires replica sync (same as 04146_timeseries_attach_detach.sql).

-- Test: the optional "histograms" target of the TimeSeries engine (Prometheus native histograms).

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_plain;
DROP TABLE IF EXISTS ts_hist;
DROP TABLE IF EXISTS ts_hist_clause;
DROP TABLE IF EXISTS ts_ext;
DROP TABLE IF EXISTS ts_ext_missing;
DROP TABLE IF EXISTS ts_ext_badtype;
DROP TABLE IF EXISTS hist_data;
DROP TABLE IF EXISTS hist_missing_column;
DROP TABLE IF EXISTS hist_bad_type;

SELECT '-- a TimeSeries table without histograms works as before';
CREATE TABLE ts_plain ENGINE = TimeSeries;
SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'ts_plain' AND name = 'histograms';
SELECT create_table_query LIKE '%HISTOGRAMS%' FROM system.tables WHERE database = currentDatabase() AND name = 'ts_plain';
SELECT count() FROM timeSeriesHistograms(ts_plain); -- { serverError UNKNOWN_TABLE }

SELECT '-- a pre-histogram definition survives DETACH/ATTACH unchanged';
DETACH TABLE ts_plain;
ATTACH TABLE ts_plain;
SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'ts_plain' AND name = 'histograms';
DROP TABLE ts_plain;

SELECT '-- store_native_histograms = 1 creates the histograms target and the outer column';
CREATE TABLE ts_hist ENGINE = TimeSeries SETTINGS store_native_histograms = 1;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'ts_hist' AND name = 'histograms';
SELECT create_table_query LIKE '%HISTOGRAMS INNER COLUMNS%' FROM system.tables WHERE database = currentDatabase() AND name = 'ts_hist';
SELECT '-- the inner histograms table schema';
SELECT name, trim(concat(type, ' ', compression_codec)) FROM system.columns WHERE database = currentDatabase() AND table LIKE '.inner_id.histograms.%' ORDER BY position;
SELECT engine, sorting_key FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.histograms.%';
SELECT count() FROM timeSeriesHistograms(ts_hist);

SELECT '-- INSERT through the outer histograms column, read back via timeSeriesHistograms()';
INSERT INTO ts_hist (metric_name, tags, histograms) VALUES
    ('test_histogram_seconds', map('job', 'test'), [('2024-01-01 00:00:01.000', 0, 3, 0.001, 10, 25.5, 2, [(0, 2), (1, 1)], [3, 2, 3], [], [], []), ('2024-01-01 00:00:02.000', 1, -53, 0, 6.5, 12.25, 0, [(0, 2)], [4.5, 2], [], [], [0.1, 0.5])]);
SELECT timestamp, flags, schema, zero_threshold, count, sum, zero_count, positive_spans, positive_values, negative_spans, negative_values, custom_values
    FROM timeSeriesHistograms(ts_hist) ORDER BY timestamp;

SELECT '-- histogram rows share the series id with the tags table';
SELECT t.metric_name, t.tags['job'], count()
    FROM timeSeriesHistograms(ts_hist) AS h
    JOIN timeSeriesTags(ts_hist) AS t ON h.id = t.id
    GROUP BY t.metric_name, t.tags['job'];

SELECT '-- min_time and max_time in the tags table come from histogram timestamps too';
SELECT min(min_time), max(max_time) FROM timeSeriesTags(ts_hist) WHERE metric_name = 'test_histogram_seconds';

SELECT '-- histograms without a metric name are rejected';
INSERT INTO ts_hist (metric_name, tags, histograms) VALUES ('', map(), [('2024-01-01 00:00:03.000', 0, 0, 0., 1, 1, 0, [], [], [], [], [])]); -- { serverError INCORRECT_DATA }

SELECT '-- a histogram-enabled definition survives DETACH/ATTACH unchanged';
DETACH TABLE ts_hist;
ATTACH TABLE ts_hist;
SELECT count() FROM timeSeriesHistograms(ts_hist);
DROP TABLE ts_hist;

SELECT '-- an explicit HISTOGRAMS INNER ENGINE clause enables the target without the setting';
CREATE TABLE ts_hist_clause ENGINE = TimeSeries HISTOGRAMS INNER ENGINE = MergeTree ORDER BY (id, timestamp);
SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'ts_hist_clause' AND name = 'histograms';
SELECT count() FROM timeSeriesHistograms(ts_hist_clause);
DROP TABLE ts_hist_clause;

SELECT '-- an external histograms table must have all the columns with the exact types';
CREATE TABLE hist_data
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    flags UInt8,
    `schema` Int8,
    zero_threshold Float64,
    count Float64,
    sum Float64,
    zero_count Float64,
    positive_spans Array(Tuple(offset Int32, length UInt32)),
    positive_values Array(Float64),
    negative_spans Array(Tuple(offset Int32, length UInt32)),
    negative_values Array(Float64),
    custom_values Array(Float64)
) ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE ts_ext ENGINE = TimeSeries HISTOGRAMS hist_data;
SELECT count() FROM timeSeriesHistograms(ts_ext);
DROP TABLE ts_ext;

CREATE TABLE hist_missing_column (id Tuple(UInt64, UUID), timestamp DateTime64(3), flags UInt8) ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE ts_ext_missing ENGINE = TimeSeries HISTOGRAMS hist_missing_column; -- { serverError THERE_IS_NO_COLUMN }

CREATE TABLE hist_bad_type
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    flags UInt8,
    `schema` Int32,
    zero_threshold Float64,
    count Float64,
    sum Float64,
    zero_count Float64,
    positive_spans Array(Tuple(offset Int32, length UInt32)),
    positive_values Array(Float64),
    negative_spans Array(Tuple(offset Int32, length UInt32)),
    negative_values Array(Float64),
    custom_values Array(Float64)
) ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE ts_ext_badtype ENGINE = TimeSeries HISTOGRAMS hist_bad_type; -- { serverError BAD_TYPE_OF_FIELD }

DROP TABLE hist_data;
DROP TABLE hist_missing_column;
DROP TABLE hist_bad_type;

SELECT '-- the SQL insert surface rejects payloads readers cannot decode';
CREATE TABLE ts_validate ENGINE = TimeSeries SETTINGS store_native_histograms = 1;

-- Spans cover 2 buckets but only 1 value is given.
INSERT INTO ts_validate (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 0, 0, 0., 1., 1., 0., [(0, 2)], [1.], [], [], [])]); -- { serverError INCORRECT_DATA }
-- A negative bucket count.
INSERT INTO ts_validate (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 0, 0, 0., 1., 1., 0., [(0, 1)], [-1.], [], [], [])]); -- { serverError INCORRECT_DATA }
-- An undefined bucket schema (between the custom-bucket value and the exponential range).
INSERT INTO ts_validate (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 0, -20, 0., 1., 1., 0., [], [], [], [], [])]); -- { serverError INCORRECT_DATA }
-- Custom bucket bounds on an exponential schema.
INSERT INTO ts_validate (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 0, 0, 0., 1., 1., 0., [], [], [], [], [1., 2.])]); -- { serverError INCORRECT_DATA }
-- Custom buckets reaching past the bounds they declare.
INSERT INTO ts_validate (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 0, -53, 0., 2., 3., 0., [(0, 2)], [1., 1.], [], [], [])]); -- { serverError INCORRECT_DATA }
-- Custom buckets with negative buckets.
INSERT INTO ts_validate (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 0, -53, 0., 1., 1., 0., [], [], [(0, 1)], [1.], [1., 2.])]); -- { serverError INCORRECT_DATA }
-- A custom-bucket histogram with a negative bucket index (no -Inf lower bound exists there).
INSERT INTO ts_validate (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 0, -53, 0., 1., 1., 0., [(-1, 1)], [1.], [], [], [1., 2.])]); -- { serverError INCORRECT_DATA }
-- An unknown flag bit.
INSERT INTO ts_validate (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 8, 0, 0., 1., 1., 0., [], [], [], [], [])]); -- { serverError INCORRECT_DATA }
-- A negative count.
INSERT INTO ts_validate (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 0, 0, 0., -1., 1., 0., [], [], [], [], [])]); -- { serverError INCORRECT_DATA }
-- A negative zero count.
INSERT INTO ts_validate (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 0, 0, 0., 1., 1., -1., [], [], [], [], [])]); -- { serverError INCORRECT_DATA }
-- A negative zero threshold.
INSERT INTO ts_validate (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 0, 0, -0.001, 1., 1., 0., [], [], [], [], [])]); -- { serverError INCORRECT_DATA }

-- The same payloads with the invariants held are accepted.
INSERT INTO ts_validate (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 0, -53, 0., 2., 3., 0., [(0, 2)], [1., 1.], [], [], [1., 2.])]);
SELECT count() FROM timeSeriesHistograms(ts_validate);
DROP TABLE ts_validate;
