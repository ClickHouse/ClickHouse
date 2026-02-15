-- Tags: no-fasttest
-- Tag no-fasttest: requires TimeSeries engine

SET allow_experimental_time_series_table = 1;

-- Create external target tables so we can query them directly to verify INSERTs.
DROP TABLE IF EXISTS ts_insert_test;
DROP TABLE IF EXISTS ts_insert_data;
DROP TABLE IF EXISTS ts_insert_tags;
DROP TABLE IF EXISTS ts_insert_metrics;

CREATE TABLE ts_insert_data
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp);

CREATE TABLE ts_insert_tags
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` Nullable(DateTime64(3)),
    `max_time` Nullable(DateTime64(3))
)
ENGINE = ReplacingMergeTree
ORDER BY (metric_name, id);

CREATE TABLE ts_insert_metrics
(
    `metric_family_name` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name;

CREATE TABLE ts_insert_test ENGINE = TimeSeries
    DATA ts_insert_data
    TAGS ts_insert_tags
    METRICS ts_insert_metrics;

-- Test 1: Basic INSERT
INSERT INTO ts_insert_test (metric_name, tags, timestamp, value)
    VALUES ('cpu_usage', {'host':'server1','region':'us-east'}, '2024-01-01 00:00:00', 42.5);

INSERT INTO ts_insert_test (metric_name, tags, timestamp, value)
    VALUES ('cpu_usage', {'host':'server2','region':'us-west'}, '2024-01-01 00:00:00', 73.1);

INSERT INTO ts_insert_test (metric_name, tags, timestamp, value)
    VALUES ('mem_usage', {'host':'server1','region':'us-east'}, '2024-01-01 00:00:00', 8192);

-- Verify 3 data rows
SELECT 'data_count_1', count() FROM ts_insert_data;

-- Verify metric names in tags table (use FINAL for ReplacingMergeTree dedup)
SELECT 'metrics', metric_name FROM ts_insert_tags FINAL ORDER BY metric_name;

-- Test 2: Multi-row INSERT
INSERT INTO ts_insert_test (metric_name, tags, timestamp, value) VALUES
    ('disk_io', {'host':'server1','device':'sda'}, '2024-01-01 00:01:00', 100),
    ('disk_io', {'host':'server1','device':'sda'}, '2024-01-01 00:02:00', 150),
    ('disk_io', {'host':'server1','device':'sdb'}, '2024-01-01 00:01:00', 200);

-- Total 6 data rows
SELECT 'data_count_2', count() FROM ts_insert_data;

-- 5 unique series after dedup (FINAL)
SELECT 'unique_series', count() FROM ts_insert_tags FINAL;

-- Test 3: Verify values
SELECT 'values', value FROM ts_insert_data ORDER BY value;

-- Test 4: Same series, new data point - should not add a new unique series
INSERT INTO ts_insert_test (metric_name, tags, timestamp, value)
    VALUES ('cpu_usage', {'host':'server1','region':'us-east'}, '2024-01-01 00:05:00', 55);

SELECT 'data_count_3', count() FROM ts_insert_data;
SELECT 'unique_series_final', count() FROM ts_insert_tags FINAL;

-- Cleanup
DROP TABLE ts_insert_test;
DROP TABLE ts_insert_data;
DROP TABLE ts_insert_tags;
DROP TABLE ts_insert_metrics;
