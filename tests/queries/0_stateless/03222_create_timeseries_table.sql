SET allow_experimental_time_series_table = 1;

CREATE TABLE 03222_timeseries_table1 ENGINE = TimeSeries FORMAT Null;
CREATE TABLE 03222_timeseries_table2 ENGINE = TimeSeries SETTINGS store_min_time_and_max_time = 1, aggregate_min_time_and_max_time = 1 FORMAT Null;

--- This doesn't work because allow_nullable_key cannot be set in query for the internal MergeTree tables
--- CREATE TABLE 03222_timeseries_table3 ENGINE = TimeSeries SETTINGS store_min_time_and_max_time = 1, aggregate_min_time_and_max_time = 0;
CREATE TABLE 03222_timeseries_table4 ENGINE = TimeSeries SETTINGS store_min_time_and_max_time = 0 FORMAT Null;
