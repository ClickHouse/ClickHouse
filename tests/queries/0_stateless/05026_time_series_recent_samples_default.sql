-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_default;

SELECT '-- a TimeSeries table gets the recent samples table by default';

CREATE TABLE ts_default ENGINE = TimeSeries;

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%';

SELECT '-- the default TTL of 4 days is pinned into the table definition at CREATE time';

SELECT create_table_query LIKE '%recent_samples_ttl_seconds = 345600%' FROM system.tables WHERE database = currentDatabase() AND name = 'ts_default';

SELECT '-- the recent samples inner table gets the default 5-hour partitioning, the pinned TTL and ttl_only_drop_parts';

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
