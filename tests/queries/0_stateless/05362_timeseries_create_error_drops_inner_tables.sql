-- A `CREATE` of a `TimeSeries` table that fails after some of its inner tables are created drops them again: the invalid
-- `recent_samples_partition_by` lets the `samples` inner table be created and then fails on the `recent samples` one.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;

CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_partition_by = 'toStartOfHour(time)'; -- { serverError UNKNOWN_IDENTIFIER }
SELECT count() FROM system.tables WHERE database = currentDatabase();

CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_partition_by = 'toStartOfHour(time)'; -- { serverError UNKNOWN_IDENTIFIER }
SELECT count() FROM system.tables WHERE database = currentDatabase();

-- A valid `CREATE` still creates the table with its inner tables.
CREATE TABLE ts ENGINE = TimeSeries;
SELECT count() > 1 FROM system.tables WHERE database = currentDatabase();

DROP TABLE ts;
