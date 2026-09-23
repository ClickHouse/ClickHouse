SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

-- Neither a collapsing engine nor plain bounds may discard part of a series' interval.
CREATE TABLE ts_bad_bounds ENGINE = TimeSeries
    TAGS MIN MAX INNER ENGINE = ReplacingMergeTree ORDER BY (metric_name, id); -- { serverError INCORRECT_QUERY }
CREATE TABLE ts_bad_bounds ENGINE = TimeSeries
    TAGS MIN MAX INNER ENGINE = MergeTree ORDER BY (metric_name, id); -- { serverError INCORRECT_QUERY }
CREATE TABLE ts_bad_bounds ENGINE = TimeSeries
    TAGS MIN MAX INNER COLUMNS (min_time Nullable(DateTime64(3)), max_time Nullable(DateTime64(3))); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts_bad_bounds ENGINE = TimeSeries
    TAGS MIN MAX INNER COLUMNS (min_time Nullable(DateTime64(3)))
    TAGS MIN MAX INNER ENGINE = AggregatingMergeTree ORDER BY (metric_name, id); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts_bad_bounds ENGINE = TimeSeries
    TAGS MIN MAX INNER COLUMNS (max_time Nullable(DateTime64(3))); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts_bad_bounds ENGINE = TimeSeries
    TAGS MIN MAX INNER COLUMNS (min_time AggregateFunction(min, Nullable(DateTime64(3)))); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts_bad_bounds ENGINE = TimeSeries
    TAGS MIN MAX INNER COLUMNS (min_time SimpleAggregateFunction(max, Nullable(DateTime64(3)))); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts_bad_bounds ENGINE = TimeSeries
    TAGS MIN MAX INNER COLUMNS (max_time SimpleAggregateFunction(min, Nullable(DateTime64(3)))); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts_bad_bounds ENGINE = TimeSeries
    TAGS MIN MAX INNER COLUMNS (min_time SimpleAggregateFunction(min, DateTime64(3))); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts_bad_bounds ENGINE = TimeSeries
    TAGS MIN MAX INNER COLUMNS (min_time SimpleAggregateFunction(min, Nullable(DateTime64(6)))); -- { serverError BAD_TYPE_OF_FIELD }

-- External targets have the same contract.
CREATE TABLE ext_bad_engine
(
    id UInt64, metric_name String,
    min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    max_time SimpleAggregateFunction(max, Nullable(DateTime64(3)))
) ENGINE = Memory;
CREATE TABLE ts_bad_bounds ENGINE = TimeSeries SETTINGS id_type = 'UInt64'
    TAGS MIN MAX ext_bad_engine; -- { serverError INCORRECT_QUERY }
DROP TABLE ext_bad_engine;

CREATE TABLE ext_bad_columns
(
    id UInt64, metric_name String,
    min_time Nullable(DateTime64(3)), max_time Nullable(DateTime64(3))
) ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1;
CREATE TABLE ts_bad_bounds ENGINE = TimeSeries SETTINGS id_type = 'UInt64'
    TAGS MIN MAX ext_bad_columns; -- { serverError BAD_TYPE_OF_FIELD }
DROP TABLE ext_bad_columns;

-- A valid customized target preserves both ends after merging disjoint batches.
CREATE TABLE ts_good_bounds ENGINE = TimeSeries SETTINGS aggregate_min_time_and_max_time = 0, recent_samples_ttl_seconds = 0
    TAGS MIN MAX INNER COLUMNS
    (
        min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))),
        max_time SimpleAggregateFunction(max, Nullable(DateTime64(3)))
    )
    TAGS MIN MAX INNER ENGINE = AggregatingMergeTree ORDER BY (metric_name, id);
INSERT INTO ts_good_bounds (metric_name, tags, samples) VALUES ('m', {'job':'api'}, [(toDateTime64(1000, 3), 1)]);
INSERT INTO ts_good_bounds (metric_name, tags, samples) VALUES ('m', {'job':'api'}, [(toDateTime64(2000, 3), 2)]);
SELECT min_time, max_time FROM timeSeriesTagsMinMax(ts_good_bounds) FINAL;
CREATE TABLE ts_good_bounds_copy AS ts_good_bounds;
INSERT INTO ts_good_bounds_copy (metric_name, tags, samples) VALUES ('m', {'job':'api'}, [(toDateTime64(1000, 3), 1)]);
INSERT INTO ts_good_bounds_copy (metric_name, tags, samples) VALUES ('m', {'job':'api'}, [(toDateTime64(2000, 3), 2)]);
SELECT min_time, max_time FROM timeSeriesTagsMinMax(ts_good_bounds_copy) FINAL;
DROP TABLE ts_good_bounds_copy;
DROP TABLE ts_good_bounds;

CREATE TABLE ext_good_bounds
(
    id UInt64, metric_name String,
    min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    max_time SimpleAggregateFunction(max, Nullable(DateTime64(3)))
) ENGINE = AggregatingMergeTree ORDER BY (metric_name, id);
CREATE TABLE ts_external_bounds ENGINE = TimeSeries SETTINGS id_type = 'UInt64', recent_samples_ttl_seconds = 0
    TAGS MIN MAX ext_good_bounds;
INSERT INTO ts_external_bounds (metric_name, tags, samples) VALUES ('m', {'job':'api'}, [(toDateTime64(1000, 3), 1)]);
INSERT INTO ts_external_bounds (metric_name, tags, samples) VALUES ('m', {'job':'api'}, [(toDateTime64(2000, 3), 2)]);
OPTIMIZE TABLE ext_good_bounds FINAL;
SELECT min_time, max_time FROM ext_good_bounds;
DROP TABLE ts_external_bounds;
DROP TABLE ext_good_bounds;
