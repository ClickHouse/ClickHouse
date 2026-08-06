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

-- Series 1: non-monotone values (so max/min/avg/last all differ), series 2: constant, series 3: a single sample.
INSERT INTO tags_table (id, metric_name, tags, min_time, max_time) VALUES
    (1, 'm', map('job', 'a'), toDateTime64(0, 3), toDateTime64(1000, 3)),
    (2, 'm', map('job', 'b'), toDateTime64(0, 3), toDateTime64(1000, 3)),
    (3, 'm', map('job', 'c'), toDateTime64(0, 3), toDateTime64(1000, 3));

INSERT INTO samples_table (id, timestamp, value)
    SELECT 1, toDateTime64(100 + 10 * number, 3), [1., 4., 2., 8., 5., 7., 3.][number + 1] FROM numbers(7);
INSERT INTO samples_table (id, timestamp, value)
    SELECT 2, toDateTime64(100 + 10 * number, 3), 10. FROM numbers(7);
INSERT INTO samples_table (id, timestamp, value) VALUES (3, toDateTime64(155, 3), 42.);

-- Single-point grid at 160 with a 60s window (100, 160]: series 1 sees {4,2,8,5,7,3}, series 3 sees {42}.
SELECT 'standalone';
SELECT id,
    timeSeriesMaxToGrid(160, 160, 15, 60)(timestamp, value),
    timeSeriesMinToGrid(160, 160, 15, 60)(timestamp, value),
    timeSeriesAvgToGrid(160, 160, 15, 60)(timestamp, value),
    timeSeriesSumToGrid(160, 160, 15, 60)(timestamp, value),
    timeSeriesCountToGrid(160, 160, 15, 60)(timestamp, value),
    timeSeriesPresentToGrid(160, 160, 15, 60)(timestamp, value)
FROM samples_table GROUP BY id ORDER BY id;

-- Empty window: the grid point at 90 has no samples in (30, 90] -> NULL.
SELECT 'standalone empty window';
SELECT id, timeSeriesMaxToGrid(90, 90, 15, 60)(timestamp, value) FROM samples_table GROUP BY id ORDER BY id;

SELECT 'instant max';
SELECT tags, value FROM prometheusQuery('ts', 'max_over_time(m[60s])', 160) ORDER BY tags;

SELECT 'instant min';
SELECT tags, value FROM prometheusQuery('ts', 'min_over_time(m[60s])', 160) ORDER BY tags;

SELECT 'instant avg';
SELECT tags, value FROM prometheusQuery('ts', 'avg_over_time(m[60s])', 160) ORDER BY tags;

SELECT 'instant sum';
SELECT tags, value FROM prometheusQuery('ts', 'sum_over_time(m[60s])', 160) ORDER BY tags;

SELECT 'instant count';
SELECT tags, value FROM prometheusQuery('ts', 'count_over_time(m[60s])', 160) ORDER BY tags;

SELECT 'instant present';
SELECT tags, value FROM prometheusQuery('ts', 'present_over_time(m[60s])', 160) ORDER BY tags;

-- (140, 160] holds exactly {7, 3} for series 1 (mean 5, population variance 4 - binary-exact in Welford),
-- {10, 10} for series 2 (variance 0) and the single sample {42} for series 3 (variance 0).
SELECT 'instant stddev';
SELECT tags, value FROM prometheusQuery('ts', 'stddev_over_time(m[20s])', 160) ORDER BY tags;

SELECT 'instant stdvar';
SELECT tags, value FROM prometheusQuery('ts', 'stdvar_over_time(m[20s])', 160) ORDER BY tags;

-- Range evaluation: grid {120, 140, 160}, window 40s; series 3 only enters the last point's window.
SELECT 'range avg';
SELECT tags, arrayMap(p -> (toUnixTimestamp64Milli(p.1), p.2), time_series) FROM prometheusQueryRange('ts', 'avg_over_time(m[40s])', 120, 160, 20) ORDER BY tags;

SELECT 'range max';
SELECT tags, arrayMap(p -> (toUnixTimestamp64Milli(p.1), p.2), time_series) FROM prometheusQueryRange('ts', 'max_over_time(m[40s])', 120, 160, 20) ORDER BY tags;

SELECT 'range count';
SELECT tags, arrayMap(p -> (toUnixTimestamp64Milli(p.1), p.2), time_series) FROM prometheusQueryRange('ts', 'count_over_time(m[40s])', 120, 160, 20) ORDER BY tags;

DROP TABLE ts;
DROP TABLE tags_table;
DROP TABLE samples_table;
