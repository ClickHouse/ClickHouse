-- Tags: no-fasttest
-- no-fasttest: Test requires ANTLR4, which is disabled in FastTest job.

-- `timeSeriesSelector` with parallel replicas returns the same samples as without them, and the tags of every returned
-- series are collected on the initiator: they are stored in the query context, so tags stored by a replica reading
-- a part of the tags table would go to its own query context, and `timeSeriesIdToTags` would fail with
-- "Unknown identifier". Checked for a table without histograms (version 7) and a table with histograms.
-- Without the local plan the initiator reads nothing itself, so every target table is read in the query contexts of
-- the replicas regardless of how the coordinator assigns the ranges.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_v7;
DROP TABLE IF EXISTS ts_hist;

CREATE TABLE ts_v7 ENGINE = TimeSeries SETTINGS version = 7, recent_samples_ttl_seconds = 0;
CREATE TABLE ts_hist ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;

INSERT INTO ts_v7 (metric_name, tags, samples)
    SELECT 'm', map('instance', toString(number)), [(toDateTime64('2026-01-01 00:00:00', 3), toFloat64(number))] FROM numbers(12);

INSERT INTO ts_hist (metric_name, tags, samples)
    SELECT 'm', map('instance', toString(number)), [(toDateTime64('2026-01-01 00:00:00', 3), toFloat64(number))] FROM numbers(12);
INSERT INTO ts_hist (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
    SELECT 'm', map('instance', toString(100 + number)), [toDateTime64('2026-01-01 00:00:00', 3)], [3], [1.5], [[(0, 1)]], [[3]] FROM numbers(6);

SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0;
-- The mode 2 only collects statistics and doesn't read with parallel replicas.
SET automatic_parallel_replicas_mode = 0;

SELECT '--- without histograms: a whole metric ---';
SELECT timeSeriesIdToTags(id) AS tags, timestamp, value
FROM timeSeriesSelector(ts_v7, 'm', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3))
ORDER BY tags;

SELECT '--- without histograms: a part of a metric ---';
SELECT timeSeriesIdToTags(id) AS tags, timestamp, value
FROM timeSeriesSelector(ts_v7, 'm{instance=~"1.*"}', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3))
ORDER BY tags;

SELECT '--- with histograms: a whole metric ---';
SELECT timeSeriesIdToTags(id) AS tags, timestamp, value, length(histogram)
FROM timeSeriesSelector(ts_hist, 'm', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3))
ORDER BY tags;

SELECT '--- with histograms: a part of a metric ---';
SELECT timeSeriesIdToTags(id) AS tags, timestamp, value, length(histogram)
FROM timeSeriesSelector(ts_hist, 'm{instance=~"1.*"}', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3))
ORDER BY tags;

-- A selector matching no series reads no data table.
SELECT '--- no matching series: the matchers select none ---';
SELECT count() FROM timeSeriesSelector(ts_v7, 'm{instance="none"}', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3));
SELECT count() FROM timeSeriesSelector(ts_hist, 'm{instance="none"}', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3));

SELECT '--- no matching series: no series has samples in the time range ---';
SELECT count() FROM timeSeriesSelector(ts_v7, 'm', toDateTime64('2025-01-01 00:00:00', 3), toDateTime64('2025-01-01 00:10:00', 3));
SELECT count() FROM timeSeriesSelector(ts_hist, 'm', toDateTime64('2025-01-01 00:00:00', 3), toDateTime64('2025-01-01 00:10:00', 3));

DROP TABLE ts_v7;
DROP TABLE ts_hist;
