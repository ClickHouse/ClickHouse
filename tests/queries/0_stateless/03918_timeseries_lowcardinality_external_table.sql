-- Tags: no-fasttest
-- Test that external TimeSeries tables with LowCardinality columns work correctly.
-- Previously, LowCardinality columns in external metrics tables caused an error.
-- Also verifies that is_inner_table is correctly determined for external tables.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_lc_test;
DROP TABLE IF EXISTS ts_lc_test_data;
DROP TABLE IF EXISTS ts_lc_test_tags;
DROP TABLE IF EXISTS ts_lc_test_metrics;

-- Create external tables with LowCardinality columns
CREATE TABLE ts_lc_test_data
(
    id UUID,
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE ts_lc_test_tags
(
    id UUID,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    all_tags Map(String, String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = AggregatingMergeTree ORDER BY (id, metric_name);

CREATE TABLE ts_lc_test_metrics
(
    metric_family_name LowCardinality(String),
    type LowCardinality(String),
    unit LowCardinality(String),
    help String
) ENGINE = ReplacingMergeTree ORDER BY (metric_family_name, type, unit);

-- This should NOT fail (previously threw SUPPORT_IS_DISABLED error)
CREATE TABLE ts_lc_test ENGINE = TimeSeries
    DATA ts_lc_test_data
    TAGS ts_lc_test_tags
    METRICS ts_lc_test_metrics;

SELECT 'OK: TimeSeries table with LowCardinality external tables created successfully';

DROP TABLE ts_lc_test;
DROP TABLE ts_lc_test_metrics;
DROP TABLE ts_lc_test_tags;
DROP TABLE ts_lc_test_data;
