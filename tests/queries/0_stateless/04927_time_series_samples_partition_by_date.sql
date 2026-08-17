-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: `DatabaseReplicated::dropTable` does not drop `TimeSeries` inner tables
-- synchronously, so the deferred inner DROPs are rejected with "ON CLUSTER is not allowed for Replicated database".

-- The auto-created inner samples table is partitioned by day (`PARTITION BY toDate(timestamp)`):
-- a time-range read then loads the primary index and marks of the touched days only, and
-- outdated data can be dropped by partition. Controlled by the `samples_partition_by_date`
-- setting; an explicitly declared samples engine is never modified.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_part_default;
DROP TABLE IF EXISTS ts_part_off;
DROP TABLE IF EXISTS ts_part_explicit;

SELECT '-- default: the samples inner table is partitioned by day';

CREATE TABLE ts_part_default ENGINE = TimeSeries;

SELECT partition_key FROM system.tables
WHERE database = currentDatabase()
  AND name = '.inner_id.samples.' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_part_default');

INSERT INTO ts_part_default (metric_name, tags, time_series) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64('2026-01-01 00:00:01', 3), 1.), (toDateTime64('2026-01-02 00:00:01', 3), 2.), (toDateTime64('2026-01-03 00:00:01', 3), 3.)]);

SELECT count() AS parts_one_per_day FROM system.parts
WHERE database = currentDatabase() AND active
  AND table = '.inner_id.samples.' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_part_default');

SELECT * FROM prometheusQuery(ts_part_default, 'foo', toDateTime64('2026-01-03 00:00:30', 3)) ORDER BY ALL;

SELECT '-- samples_partition_by_date = 0: no partitioning';

CREATE TABLE ts_part_off ENGINE = TimeSeries SETTINGS samples_partition_by_date = 0;

SELECT partition_key = '' FROM system.tables
WHERE database = currentDatabase()
  AND name = '.inner_id.samples.' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_part_off');

SELECT '-- an explicitly declared samples engine is kept as is';

CREATE TABLE ts_part_explicit ENGINE = TimeSeries SAMPLES INNER ENGINE = MergeTree() ORDER BY (id, timestamp);

SELECT partition_key = '' FROM system.tables
WHERE database = currentDatabase()
  AND name = '.inner_id.samples.' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_part_explicit');

DROP TABLE ts_part_default;
DROP TABLE ts_part_off;
DROP TABLE ts_part_explicit;
