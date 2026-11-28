CREATE TABLE ts_raw_data(timestamp DateTime64(3,'UTC'), value Float64) ENGINE = MergeTree() ORDER BY timestamp;

INSERT INTO ts_raw_data SELECT arrayJoin(*).1::DateTime64(3, 'UTC') AS timestamp, arrayJoin(*).2 AS value
FROM (
select [
(1734955421.020, 0),
(1734955436.020, 5),
(1734955451.020, 3),
(1734955451.020, 2),
(1734955435.020, 4),
(1734955436.020, 3),
(1734955511.020, 5)
]);

SELECT * FROM ts_raw_data;

SET allow_experimental_time_series_aggregate_functions = 1;

SELECT 'groupArray: ', timeSeriesGroupArray(timestamp, value) FROM ts_raw_data;

DROP TABLE ts_raw_data;
