SET allow_experimental_time_series_table = 1;

CREATE TABLE 03222_timeseries_table1 ENGINE = TimeSeries FORMAT Null;
CREATE TABLE 03222_timeseries_table2 ENGINE = TimeSeries SETTINGS store_time_ranges = 0 FORMAT Null;

-- The settings of the columns `min_time` and `max_time` of the tags table apply to tables of versions before 7.
CREATE TABLE 03222_timeseries_table3 ENGINE = TimeSeries SETTINGS version = 6, store_min_time_and_max_time = 1, aggregate_min_time_and_max_time = 1 FORMAT Null;
CREATE TABLE 03222_timeseries_table4 ENGINE = TimeSeries SETTINGS version = 6, store_min_time_and_max_time = 1, aggregate_min_time_and_max_time = 0 FORMAT Null;
CREATE TABLE 03222_timeseries_table5 ENGINE = TimeSeries SETTINGS version = 6, store_min_time_and_max_time = 0 FORMAT Null;
