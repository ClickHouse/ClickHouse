-- Test: exercises `TimeSeriesColumnsValidator` error paths for incompatible column types
-- Covers: src/Storages/TimeSeries/TimeSeriesColumnsValidator.cpp:175-250 — column type validation

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_bad_timestamp;
DROP TABLE IF EXISTS ts_bad_value;
DROP TABLE IF EXISTS ts_bad_tags;
DROP TABLE IF EXISTS ts_bad_metric_name;

-- Test: timestamp must be DateTime64, not String
CREATE TABLE ts_bad_timestamp (timestamp String) ENGINE = TimeSeries; -- { serverError INCOMPATIBLE_COLUMNS }

-- Test: value must be Float32/Float64, not String
CREATE TABLE ts_bad_value (value String) ENGINE = TimeSeries; -- { serverError INCOMPATIBLE_COLUMNS }

-- Test: tags must be Map(String, String), not String
CREATE TABLE ts_bad_tags (tags String) ENGINE = TimeSeries; -- { serverError INCOMPATIBLE_COLUMNS }

-- Test: metric_name must be String, not Int32
CREATE TABLE ts_bad_metric_name (metric_name Int32) ENGINE = TimeSeries; -- { serverError INCOMPATIBLE_COLUMNS }

-- Verify valid table creation still works
DROP TABLE IF EXISTS ts_valid;
CREATE TABLE ts_valid ENGINE = TimeSeries;
SELECT 'ok';
DROP TABLE ts_valid;
