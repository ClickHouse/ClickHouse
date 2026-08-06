-- Tags: no-fasttest, no-replicated-database
-- ^^ the PromQL grammar needs ANTLR4 (disabled in fast-test), and TimeSeries does not round-trip through DatabaseReplicated.

SET allow_experimental_time_series_table = 1;
SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS samples_table;
DROP TABLE IF EXISTS tags_table;

CREATE TABLE samples_table (id UInt64, timestamp DateTime64(3, 'UTC'), value Float64)
ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE tags_table (
    id UInt64,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))),
    max_time SimpleAggregateFunction(max, Nullable(DateTime64(3, 'UTC'))))
ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1;

CREATE TABLE ts ENGINE = TimeSeries SAMPLES samples_table TAGS tags_table;

-- `m`: non-monotone values / a constant / a single sample (for quantile_over_time);
-- `lin`: two perfectly linear series with slopes 2 and -0.5 (so predict_linear is exact);
-- `n`: contains a NaN sample (NaN sorts before any number in Prometheus's quantile).
INSERT INTO tags_table (id, metric_name, tags, min_time, max_time) VALUES
    (1, 'm', map('job', 'a'), toDateTime64(0, 3), toDateTime64(1000, 3)),
    (2, 'm', map('job', 'b'), toDateTime64(0, 3), toDateTime64(1000, 3)),
    (3, 'm', map('job', 'c'), toDateTime64(0, 3), toDateTime64(1000, 3)),
    (4, 'lin', map('job', 'd'), toDateTime64(0, 3), toDateTime64(1000, 3)),
    (5, 'lin', map('job', 'e'), toDateTime64(0, 3), toDateTime64(1000, 3)),
    (6, 'n', map('job', 'f'), toDateTime64(0, 3), toDateTime64(1000, 3));

INSERT INTO samples_table (id, timestamp, value)
    SELECT 1, toDateTime64(100 + 10 * number, 3), [1., 4., 2., 8., 5., 7., 3.][number + 1] FROM numbers(7);
INSERT INTO samples_table (id, timestamp, value)
    SELECT 2, toDateTime64(100 + 10 * number, 3), 10. FROM numbers(7);
INSERT INTO samples_table (id, timestamp, value) VALUES (3, toDateTime64(155, 3), 42.);
INSERT INTO samples_table (id, timestamp, value)
    SELECT 4, toDateTime64(100 + 10 * number, 3), 2. * (100 + 10 * number) FROM numbers(7);
INSERT INTO samples_table (id, timestamp, value)
    SELECT 5, toDateTime64(100 + 10 * number, 3), 1000. - 0.5 * (100 + 10 * number) FROM numbers(7);
INSERT INTO samples_table (id, timestamp, value) VALUES
    (6, toDateTime64(150, 3), nan), (6, toDateTime64(155, 3), 1.), (6, toDateTime64(160, 3), 2.);

-- Median over the window (100, 160]: series 1 sees {2,3,4,5,7,8} -> 4.5 by linear interpolation.
SELECT 'standalone quantile';
SELECT id, timeSeriesQuantileToGrid(160, 160, 15, 60, 0.5)(timestamp, value) FROM samples_table GROUP BY id ORDER BY id;

SELECT 'instant predict_linear';
SELECT tags, value FROM prometheusQuery('ts', 'predict_linear(lin[60s], 60)', 160) ORDER BY tags;

SELECT 'range predict_linear';
SELECT tags, arrayMap(p -> (toUnixTimestamp64Milli(p.1), p.2), time_series) FROM prometheusQueryRange('ts', 'predict_linear(lin[40s], 30)', 120, 160, 20) ORDER BY tags;

SELECT 'instant quantile 0.5';
SELECT tags, value FROM prometheusQuery('ts', 'quantile_over_time(0.5, m[60s])', 160) ORDER BY tags;

SELECT 'instant quantile 1';
SELECT tags, value FROM prometheusQuery('ts', 'quantile_over_time(1, m[60s])', 160) ORDER BY tags;

SELECT 'range quantile 0.5';
SELECT tags, arrayMap(p -> (toUnixTimestamp64Milli(p.1), p.2), time_series) FROM prometheusQueryRange('ts', 'quantile_over_time(0.5, m[40s])', 120, 160, 20) ORDER BY tags;

-- NaN samples sort before any number (Prometheus semantics): the window (100, 160] of `n` holds {NaN, 1, 2},
-- so the median interpolates between 1 and 2 with weight 0 -> 1, and quantile 0 lands on the NaN itself.
SELECT 'instant quantile 0.5 with NaN';
SELECT tags, value FROM prometheusQuery('ts', 'quantile_over_time(0.5, n[60s])', 160) ORDER BY tags;

SELECT 'instant quantile 0 with NaN';
SELECT tags, value FROM prometheusQuery('ts', 'quantile_over_time(0, n[60s])', 160) ORDER BY tags;

-- The extra scalar argument must be a constant: a scalar produced by a subexpression is rejected.
SELECT tags, value FROM prometheusQuery('ts', 'predict_linear(lin[60s], scalar(m{job="b"}))', 160); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

DROP TABLE ts;
DROP TABLE tags_table;
DROP TABLE samples_table;
