-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_selector_late_samples_read;

CREATE TABLE ts_selector_late_samples_read ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;

INSERT INTO ts_selector_late_samples_read (metric_name, tags, time_series) VALUES
    ('m', map('env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(300, 3), 3.)]),
    ('m', map('env', 'dev'), [(toDateTime64(200, 3), 2.)]);

-- The cheap conditions on the samples-table metadata must be evaluated in PREWHERE before the
-- `samples` array is read. The outer `notEmpty(time_series)` stays in a separate filter because it
-- needs the sliced samples and therefore cannot participate in the early read step.
SELECT
    countIf(explain LIKE '%Prewhere filter column:%') = 1 AS has_prewhere,
    countIf(explain LIKE '%Prewhere filter column:%timeSeriesSliceSortedArray%') = 0 AS samples_not_in_prewhere,
    countIf(explain LIKE '%Filter column: notEmpty(%timeSeriesSliceSortedArray%') = 1 AS sliced_samples_filtered_late
FROM
(
    EXPLAIN actions = 1
    SELECT id, time_series
    FROM timeSeriesSelector(ts_selector_late_samples_read, 'm{env="prod"}', 100, 250)
);

SELECT arrayJoin(time_series) AS sample
FROM timeSeriesSelector(ts_selector_late_samples_read, 'm{env="prod"}', 100, 250);

DROP TABLE ts_selector_late_samples_read;

-- External samples tables are allowed to use an engine without PREWHERE support. Keep the same
-- selector semantics there and use the optimization only when the target storage advertises it.
DROP TABLE IF EXISTS ts_selector_late_samples_memory;
DROP TABLE IF EXISTS ts_selector_late_samples_memory_data;

CREATE TABLE ts_selector_late_samples_memory_data
(
    id UUID,
    samples Array(Tuple(timestamp DateTime64(3), value Float64)),
    bucket DateTime64(3),
    min_time DateTime64(3),
    max_time DateTime64(3)
)
ENGINE = Memory;

CREATE TABLE ts_selector_late_samples_memory ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0
SAMPLES ts_selector_late_samples_memory_data;

INSERT INTO ts_selector_late_samples_memory (metric_name, tags, time_series) VALUES
    ('m', map('env', 'prod'), [(toDateTime64(100, 3), 1.)]);

SELECT arrayJoin(time_series) AS sample
FROM timeSeriesSelector(ts_selector_late_samples_memory, 'm{env="prod"}', 100, 250);

DROP TABLE ts_selector_late_samples_memory;
DROP TABLE ts_selector_late_samples_memory_data;

-- Distributed accepts PREWHERE syntax itself, but cannot guarantee that its remote target supports
-- PREWHERE. Keep WHERE for that wrapper so a remote Memory samples table remains usable.
DROP TABLE IF EXISTS ts_selector_late_samples_distributed;
DROP TABLE IF EXISTS ts_selector_late_samples_distributed_data;
DROP TABLE IF EXISTS ts_selector_late_samples_distributed_local;

CREATE TABLE ts_selector_late_samples_distributed_local
(
    id UUID,
    samples Array(Tuple(timestamp DateTime64(3), value Float64)),
    bucket DateTime64(3),
    min_time DateTime64(3),
    max_time DateTime64(3)
)
ENGINE = Memory;

CREATE TABLE ts_selector_late_samples_distributed_data AS ts_selector_late_samples_distributed_local
ENGINE = Distributed(test_shard_localhost, currentDatabase(), ts_selector_late_samples_distributed_local);

CREATE TABLE ts_selector_late_samples_distributed ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0
SAMPLES ts_selector_late_samples_distributed_data;

INSERT INTO ts_selector_late_samples_distributed (metric_name, tags, time_series)
SETTINGS insert_distributed_sync = 1
VALUES ('m', map('env', 'prod'), [(toDateTime64(100, 3), 1.)]);

SELECT arrayJoin(time_series) AS sample
FROM timeSeriesSelector(ts_selector_late_samples_distributed, 'm{env="prod"}', 100, 250);

DROP TABLE ts_selector_late_samples_distributed;
DROP TABLE ts_selector_late_samples_distributed_data;
DROP TABLE ts_selector_late_samples_distributed_local;
