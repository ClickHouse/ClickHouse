-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A grid built for an instant selector keeps Prometheus stale markers, so a range function applied to
-- a subquery over an instant selector must drop them: a stale step means "the series is absent here",
-- not "the latest sample is `NaN`".

DROP TABLE IF EXISTS prometheus;
DROP TABLE IF EXISTS tags_table;
DROP TABLE IF EXISTS samples_table;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE tags_table
(
    id UInt64,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time DateTime64(3),
    max_time DateTime64(3)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE samples_table
(
    id UInt64,
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree() ORDER BY (id, timestamp);

CREATE TABLE prometheus ENGINE = TimeSeries SAMPLES samples_table TAGS tags_table;

INSERT INTO prometheus (metric_name, tags, time_series) VALUES
    ('stale_counter', map('test_case', 'stale'), [(toDateTime64(70, 3), 1), (toDateTime64(100, 3), 2)]);

-- `0x7ff0000000000002` is Prometheus's staleness NaN bit pattern; it can only be written directly
-- into the samples table, because the remote-write path is not available here.
INSERT INTO samples_table
    SELECT id, toDateTime64(110, 3), reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(0x7ff0000000000002))) FROM tags_table;

SELECT '-- an instant selector does not return a stale step';
SELECT * FROM prometheusQuery('prometheus', 'stale_counter', 110);

SELECT '-- a range function over a subquery skips the stale step and returns the last real sample';
SELECT * FROM prometheusQuery('prometheus', 'last_over_time(stale_counter[40s:10s])', 110);

SELECT '-- the stale step does not count as a change of the value either';
SELECT * FROM prometheusQuery('prometheus', 'changes(stale_counter[40s:10s])', 110);

DROP TABLE prometheus;
DROP TABLE tags_table;
DROP TABLE samples_table;
