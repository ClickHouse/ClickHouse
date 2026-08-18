-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: the recent samples table is not supported in Replicated databases
-- (its inner materialized view would get a different UUID on every replica).

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_recent;

-- TTL 10 days. Samples are inserted close to now(), so the background TTL cannot drop them during the test.
CREATE TABLE ts_recent ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;

SELECT '-- the recent samples inner table and its materialized view exist';

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%';
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamplesmv.%';

SELECT '-- the recent samples inner table is partitioned by day, has the TTL and ttl_only_drop_parts';

SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%';

SELECT '-- the materialized view copies inserted samples into the recent samples table';

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

SELECT '-- the table survives DETACH/ATTACH: the preference and the materialized view keep working';

DETACH TABLE ts_recent;
ATTACH TABLE ts_recent;

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

SELECT '-- settings of the recent samples table require recent_samples_ttl_seconds';

CREATE TABLE ts_recent_bad ENGINE = TimeSeries SETTINGS recent_samples_partition_by = 'toStartOfHour(timestamp)'; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts_recent_bad ENGINE = TimeSeries SETTINGS recent_samples_index_granularity = 4096; -- { serverError INVALID_SETTING_VALUE }

SELECT '-- a RECENT SAMPLES clause requires recent_samples_ttl_seconds';

CREATE TABLE ts_recent_bad ENGINE = TimeSeries RECENT SAMPLES ENGINE = MergeTree ORDER BY (id, timestamp); -- { serverError INCORRECT_QUERY }

SELECT '-- DROP TABLE drops the inner tables and the materialized view';

DROP TABLE ts_recent;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%';
