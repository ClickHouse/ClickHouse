-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: `DatabaseReplicated` does not drop `TimeSeries` inner tables synchronously; deferred DROPs are rejected.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_recent;

CREATE TABLE ts_recent ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;

SELECT '-- the recent samples inner table exists';

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%';

SELECT '-- the recent samples inner table is partitioned by 5-hour buckets by default and has no wall-clock TTL';

SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%';

SELECT '-- inserted samples are written to the recent samples table as well';

INSERT INTO ts_recent (metric_name, tags, time_series) VALUES
    ('test_metric', map('env', 'prod'), [(now64(3) - INTERVAL 3 MINUTE, 42.), (now64(3) - INTERVAL 2 MINUTE, 43.)]),
    ('test_metric', map('env', 'dev'), [(now64(3) - INTERVAL 2 MINUTE, 100.)]);

SELECT
    (SELECT sum(total_rows) FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.samples.%') AS samples_rows,
    (SELECT sum(total_rows) FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%') AS recent_rows;

SELECT '-- a query fitting in the TTL window reads from the recent samples table and returns the same data';

SELECT plan LIKE '%.inner_id.recentsamples.%' AS reads_recent, plan LIKE '%.inner_id.samples.%' AS reads_main
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN SELECT sum(value) FROM prometheusQuery(ts_recent, 'test_metric', now())));

SELECT value FROM prometheusQuery(ts_recent, 'test_metric', now()) ORDER BY value;
SELECT value FROM prometheusQuery(ts_recent, 'sum(test_metric)', now()) ORDER BY value;

SELECT '-- a query outside the TTL window reads from the main samples table';

SELECT plan LIKE '%.inner_id.recentsamples.%' AS reads_recent, plan LIKE '%.inner_id.samples.%' AS reads_main
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN SELECT sum(value) FROM prometheusQuery(ts_recent, 'test_metric', toDateTime64('2020-01-01 00:00:00', 3))));

SELECT '-- disabling the preference reads from the main samples table';

SELECT plan LIKE '%.inner_id.recentsamples.%' AS reads_recent, plan LIKE '%.inner_id.samples.%' AS reads_main
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN SELECT sum(value) FROM prometheusQuery(ts_recent, 'test_metric', now())))
SETTINGS time_series_prefer_recent_samples_table = 0;

SELECT value FROM prometheusQuery(ts_recent, 'test_metric', now()) ORDER BY value SETTINGS time_series_prefer_recent_samples_table = 0;

SELECT '-- the table survives DETACH/ATTACH: the preference and the write path keep working';

DETACH TABLE ts_recent;
ATTACH TABLE ts_recent;
OPTIMIZE TABLE ts_recent FINAL;

SELECT plan LIKE '%.inner_id.recentsamples.%' AS reads_recent
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN SELECT sum(value) FROM prometheusQuery(ts_recent, 'test_metric', now())));

INSERT INTO ts_recent (metric_name, tags, time_series) VALUES
    ('test_metric', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 44.)]);

SELECT
    (SELECT sum(total_rows) FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.samples.%') AS samples_rows,
    (SELECT sum(total_rows) FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%') AS recent_rows;

SELECT '-- custom partitioning and index granularity of the recent samples table';

DROP TABLE IF EXISTS ts_recent_custom;
CREATE TABLE ts_recent_custom ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 259200, recent_samples_partition_by = 'toStartOfHour(timestamp)', recent_samples_index_granularity = 4096;

SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%' AND engine_full LIKE '%toStartOfHour%';

DROP TABLE ts_recent_custom;

SELECT '-- a wall-clock TTL on a user-declared recent samples engine is removed';

DROP TABLE IF EXISTS ts_recent_declared;
CREATE TABLE ts_recent_declared ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 432000
RECENT SAMPLES ENGINE = MergeTree PARTITION BY toStartOfDay(timestamp) ORDER BY (id, timestamp) TTL toDateTime(timestamp) + toIntervalSecond(1);

SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%' AND engine_full LIKE '%toStartOfDay%';

DROP TABLE ts_recent_declared;

SELECT '-- an external recent samples table is fed by inserts and preferred by reads';

DROP TABLE IF EXISTS ts_recent_ext;
DROP TABLE IF EXISTS recent_ext;
CREATE TABLE recent_ext
(
    `id` Tuple(UInt64, UUID),
    `timestamp` DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
    `value` Float64 CODEC(ZSTD(3))
)
ENGINE = MergeTree PARTITION BY toDate(timestamp) ORDER BY (id, timestamp);

CREATE TABLE ts_recent_ext ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000 RECENT SAMPLES recent_ext;

INSERT INTO ts_recent_ext (metric_name, tags, time_series) VALUES
    ('ext_metric', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 7.)]);

SELECT count() FROM recent_ext;

SELECT plan LIKE '%recent_ext%' AS reads_recent
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN SELECT sum(value) FROM prometheusQuery(ts_recent_ext, 'ext_metric', now())));

SELECT value FROM prometheusQuery(ts_recent_ext, 'ext_metric', now());

DROP TABLE ts_recent_ext;
DROP TABLE recent_ext;

SELECT '-- settings of the recent samples table require a non-zero recent_samples_ttl_seconds';

CREATE TABLE ts_recent_bad ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0, recent_samples_partition_by = 'toStartOfHour(timestamp)'; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts_recent_bad ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0, recent_samples_index_granularity = 4096; -- { serverError INVALID_SETTING_VALUE }

SELECT '-- a RECENT SAMPLES clause requires a non-zero recent_samples_ttl_seconds';

CREATE TABLE ts_recent_bad ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0 RECENT SAMPLES ENGINE = MergeTree ORDER BY (id, timestamp); -- { serverError INCORRECT_QUERY }

SELECT '-- the recent samples inner table requires a MergeTree-family engine';

CREATE TABLE ts_recent_bad ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000 RECENT SAMPLES ENGINE = Memory; -- { serverError INVALID_SETTING_VALUE }

SELECT '-- DROP TABLE drops the inner tables';

DROP TABLE ts_recent;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%';

SELECT '-- a TimeSeries table gets the recent samples table by default';

DROP TABLE IF EXISTS ts_default;
CREATE TABLE ts_default ENGINE = TimeSeries;

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%';

SELECT '-- the default TTL of 4 days is pinned into the table definition at CREATE time';

SELECT create_table_query LIKE '%recent_samples_ttl_seconds = 345600%' FROM system.tables WHERE database = currentDatabase() AND name = 'ts_default';

SELECT '-- the recent samples inner table gets the default 5-hour partitioning without a wall-clock TTL';

SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%';

SELECT '-- inserted samples are copied to the recent samples table and short-range queries prefer it';

INSERT INTO ts_default (metric_name, tags, time_series) VALUES
    ('default_metric', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 42.)]);

SELECT
    (SELECT sum(total_rows) FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.samples.%') AS samples_rows,
    (SELECT sum(total_rows) FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%') AS recent_rows;

SELECT plan LIKE '%.inner_id.recentsamples.%' AS reads_recent
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN SELECT sum(value) FROM prometheusQuery(ts_default, 'default_metric', now())));

SELECT value FROM prometheusQuery(ts_default, 'default_metric', now());

DROP TABLE ts_default;

SELECT '-- an explicit recent_samples_ttl_seconds = 0 disables the recent samples table';

DROP TABLE IF EXISTS ts_disabled;
CREATE TABLE ts_disabled ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples%';
SELECT create_table_query LIKE '%recent_samples_ttl_seconds = 0%' FROM system.tables WHERE database = currentDatabase() AND name = 'ts_disabled';

INSERT INTO ts_disabled (metric_name, tags, time_series) VALUES
    ('default_metric', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 7.)]);

SELECT value FROM prometheusQuery(ts_disabled, 'default_metric', now());

DROP TABLE ts_disabled;

SELECT '-- DROP TABLE leaves no inner tables behind';

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%';
