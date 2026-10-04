SET allow_experimental_time_series_table = 1;

CREATE TABLE 03222_timeseries_table1 ENGINE = TimeSeries FORMAT Null;
CREATE TABLE 03222_timeseries_table2 ENGINE = TimeSeries SETTINGS store_time_ranges = 0 FORMAT Null;
