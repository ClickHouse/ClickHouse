-- Tags: no-fasttest
-- Tag no-fasttest: the TimeSeries table engine is experimental and disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
-- The fix must make the samples read in-order on its own, without the caller enabling it, so pin
-- the caller-visible setting off and keep plan optimizations on (defend against CI randomization).
SET optimize_aggregation_in_order = 0;
SET query_plan_enable_optimizations = 1;

DROP TABLE IF EXISTS ts_read_in_order;

-- recent_samples_ttl_seconds = 0 disables the recent samples table, so a direct read goes to the
-- samples inner table (ordered by (id, timestamp)).
CREATE TABLE ts_read_in_order ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;

-- Insert each series in several batches so the samples table has multiple parts. A plain hash
-- aggregation over `GROUP BY id` would then interleave each series' samples across parts and buffer
-- them all; an in-order aggregation streams one series at a time in (id, timestamp) order.
INSERT INTO ts_read_in_order (metric_name, tags, time_series) VALUES ('m', map('n', 'a'), [(toDateTime64('2024-01-01 00:00:00', 3), 1.)]), ('m', map('n', 'b'), [(toDateTime64('2024-01-01 00:00:00', 3), 10.)]);
INSERT INTO ts_read_in_order (metric_name, tags, time_series) VALUES ('m', map('n', 'a'), [(toDateTime64('2024-01-01 00:00:15', 3), 2.)]), ('m', map('n', 'b'), [(toDateTime64('2024-01-01 00:00:15', 3), 20.)]);
INSERT INTO ts_read_in_order (metric_name, tags, time_series) VALUES ('m', map('n', 'a'), [(toDateTime64('2024-01-01 00:00:30', 3), 3.)]), ('m', map('n', 'b'), [(toDateTime64('2024-01-01 00:00:30', 3), 30.)]);

SELECT '-- the samples GROUP BY id is read in order even though the caller did not set optimize_aggregation_in_order';
SELECT countSubstrings(pipeline, 'AggregatingInOrderTransform') > 0 AS reads_samples_in_order
FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS pipeline
    FROM (EXPLAIN PIPELINE SELECT time_series FROM ts_read_in_order)
);

SELECT '-- it is a streaming ordered read, not a full sort of the samples';
SELECT countSubstrings(pipeline, 'MergeSortingTransform') = 0 AS no_full_sort
FROM (
    SELECT arrayStringConcat(groupArray(explain), '\n') AS pipeline
    FROM (EXPLAIN PIPELINE SELECT time_series FROM ts_read_in_order)
);

SELECT '-- reading back returns each series with all its samples (content is unchanged; array order is not guaranteed, so normalize with arraySort)';
SELECT tags['n'] AS series, arraySort(time_series) AS samples
FROM ts_read_in_order
ORDER BY series;

DROP TABLE ts_read_in_order;
