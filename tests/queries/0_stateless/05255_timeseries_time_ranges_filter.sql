-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: every parallel replica reads all rows of the table behind `timeSeriesTimeRanges`, so each row
-- is returned once per replica until https://github.com/ClickHouse/ClickHouse/pull/119350 is merged.
--
-- The selector filters time series by the time ranges stored in the "time ranges" table (the write path and which tables
-- the selector reads are checked in 05255_timeseries_time_ranges_table.sql). A stored time range can't be changed through
-- the TimeSeries table, so a time series is written directly into the inner tables with a time range which doesn't intersect
-- its samples: the selectors trust the stored time range and hide the time series, which is the only way to see the filtering.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;

-- The identifiers are written explicitly into the inner tables, so they are declared as UInt64.
CREATE TABLE ts ENGINE = TimeSeries TAGS INNER COLUMNS (id UInt64);

-- A time series written through the table gets the time range of its samples.
INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('host', 'h2'), [(toDateTime64(2000, 3), 3.)]);

-- A time series written directly into the inner tables, with the time range [3000, 3000] far from its samples.
-- The table name is qualified because an asynchronous insert through a table function resolves an unqualified name in the default database.
INSERT INTO FUNCTION timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) (id, metric_name, tags) VALUES (1, 'm', map('__name__', 'm', 'host', 'h1'));
INSERT INTO FUNCTION timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) (id, timestamp, value) VALUES
    (1, toDateTime64(500, 3), 0.), (1, toDateTime64(1000, 3), 1.), (1, toDateTime64(1060, 3), 2.), (1, toDateTime64(1500, 3), 4.);
INSERT INTO FUNCTION timeSeriesTimeRanges({CLICKHOUSE_DATABASE:Identifier}.ts) (id, min_time, max_time) VALUES (1, toDateTime64(3000, 3), toDateTime64(3000, 3));

SELECT min_time, max_time FROM timeSeriesTimeRanges(ts) ORDER BY min_time;

-- h1 is hidden from all the selectors: without the filter the results would be 5, 4 and 4.
SELECT count() FROM timeSeriesSelector(ts, 'm', 0, 2500);
SELECT count() FROM timeSeriesSelector(ts, 'm{host="h1"}', 0, 2500);
SELECT count() FROM timeSeriesSelector(ts, 'm{host="h1"}', 0, 1500);

DROP TABLE ts;
