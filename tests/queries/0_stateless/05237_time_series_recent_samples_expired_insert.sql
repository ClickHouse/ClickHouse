-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: `DatabaseReplicated` does not drop `TimeSeries` inner tables synchronously; deferred DROPs are rejected.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_expired;

-- TTL 1 hour. The samples close to now() stay inside the TTL window for the whole test,
-- so the background TTL cannot drop them and race the row and part counts.
CREATE TABLE ts_expired ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 3600;

SELECT '-- a sample older than the TTL window goes to the samples table only';

INSERT INTO ts_expired (metric_name, tags, samples) VALUES
    ('expired_metric', map('env', 'prod'), [(now64(3) - INTERVAL 10 DAY, 1.), (now64(3) - INTERVAL 1 MINUTE, 2.)]);

SELECT
    (SELECT sum(total_rows) FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.samples.%') AS samples_rows,
    (SELECT sum(total_rows) FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%') AS recent_rows;

SELECT '-- the recent samples table holds one part, not one per partition of the expired range';

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table LIKE '.inner\_id.recentsamples.%' AND active;

SELECT '-- a block of only expired samples writes nothing to the recent samples table';

INSERT INTO ts_expired (metric_name, tags, samples) VALUES
    ('expired_metric', map('env', 'dev'), [(now64(3) - INTERVAL 20 DAY, 3.), (now64(3) - INTERVAL 15 DAY, 4.)]);

SELECT
    (SELECT sum(total_rows) FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.samples.%') AS samples_rows,
    (SELECT sum(total_rows) FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%') AS recent_rows;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table LIKE '.inner\_id.recentsamples.%' AND active;

SELECT '-- a query inside the TTL window still reads the kept sample from the recent samples table';

SELECT plan LIKE '%.inner_id.recentsamples.%' AS reads_recent, plan LIKE '%.inner_id.samples.%' AS reads_main
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN SELECT sum(value) FROM prometheusQuery(ts_expired, 'expired_metric', now())));

SELECT value FROM prometheusQuery(ts_expired, 'expired_metric', now()) ORDER BY value;

SELECT '-- a query outside the TTL window reads the expired samples from the samples table';

-- The evaluation instant is one minute after the stored sample, well inside the 5-minute lookback window,
-- so the second in which the INSERT ran cannot decide whether the sample is already in the past.
SELECT plan LIKE '%.inner_id.recentsamples.%' AS reads_recent, plan LIKE '%.inner_id.samples.%' AS reads_main
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN SELECT sum(value) FROM prometheusQuery(ts_expired, 'expired_metric', now() - toIntervalDay(15) + toIntervalMinute(1))));

SELECT value FROM prometheusQuery(ts_expired, 'expired_metric', now() - toIntervalDay(15) + toIntervalMinute(1)) ORDER BY value;
SELECT value FROM prometheusQuery(ts_expired, 'expired_metric', now() - toIntervalDay(10) + toIntervalMinute(1)) ORDER BY value;

DROP TABLE ts_expired;
