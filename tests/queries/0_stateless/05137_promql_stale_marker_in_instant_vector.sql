-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A stale marker means "the series is absent from here on", so it must not surface as a `NaN` sample
-- in ordinary instant-vector expressions either. Arithmetic, unary and comparison operators and
-- aggregations quiet the staleness NaN payload, so a marker which reaches them can no longer be
-- recognized at finalization - the grid of an instant selector normalizes the markers up front.

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

INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('stale_counter', map('test_case', 'stale'), [(toDateTime64(70, 3), 1), (toDateTime64(100, 3), 2)]);

-- `0x7ff0000000000002` is Prometheus's staleness NaN bit pattern; it can only be written directly
-- into the samples table, because the remote-write path is not available here.
INSERT INTO samples_table
    SELECT id, toDateTime64(110, 3), reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(0x7ff0000000000002))) FROM tags_table;

SELECT '-- at the stale step the series is absent for every kind of expression';
SELECT * FROM prometheusQuery('prometheus', 'stale_counter + 1', 110);
SELECT * FROM prometheusQuery('prometheus', '-stale_counter', 110);
SELECT * FROM prometheusQuery('prometheus', 'sum(stale_counter)', 110);
SELECT * FROM prometheusQuery('prometheus', 'stale_counter != bool 0', 110);
SELECT * FROM prometheusQuery('prometheus', 'abs(stale_counter)', 110);
SELECT * FROM prometheusQuery('prometheus', 'stale_counter > 0', 110);

SELECT '-- but the same expressions return the last real sample one step earlier';
SELECT * FROM prometheusQuery('prometheus', 'stale_counter + 1', 100);
SELECT * FROM prometheusQuery('prometheus', '-stale_counter', 100);
SELECT * FROM prometheusQuery('prometheus', 'sum(stale_counter)', 100);
SELECT * FROM prometheusQuery('prometheus', 'stale_counter != bool 0', 100);
SELECT * FROM prometheusQuery('prometheus', 'abs(stale_counter)', 100);
SELECT * FROM prometheusQuery('prometheus', 'stale_counter > 0', 100);

DROP TABLE prometheus;
DROP TABLE tags_table;
DROP TABLE samples_table;
