-- The `prometheus_remote_write_dynamic_routing_enabled` setting of the TimeSeries table engine exists from version 9
-- (see TimeSeriesVersion.h): a table of an earlier version must not have it, so that an older server can still attach the table.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_routing_8;
DROP TABLE IF EXISTS ts_routing_9;

CREATE TABLE ts_routing_8 ENGINE = TimeSeries SETTINGS version = 8, prometheus_remote_write_dynamic_routing_enabled = 1; -- { serverError INVALID_SETTING_VALUE }

CREATE TABLE ts_routing_8 ENGINE = TimeSeries SETTINGS version = 8;
ALTER TABLE ts_routing_8 MODIFY SETTING prometheus_remote_write_dynamic_routing_enabled = 1; -- { serverError INVALID_SETTING_VALUE }
SELECT position(create_table_query, 'prometheus_remote_write_dynamic_routing_enabled') > 0
    FROM system.tables WHERE database = currentDatabase() AND name = 'ts_routing_8';

CREATE TABLE ts_routing_9 ENGINE = TimeSeries SETTINGS version = 9;
ALTER TABLE ts_routing_9 MODIFY SETTING prometheus_remote_write_dynamic_routing_enabled = 1;
SELECT position(create_table_query, 'prometheus_remote_write_dynamic_routing_enabled = true') > 0
    FROM system.tables WHERE database = currentDatabase() AND name = 'ts_routing_9';

DROP TABLE ts_routing_8;
DROP TABLE ts_routing_9;
