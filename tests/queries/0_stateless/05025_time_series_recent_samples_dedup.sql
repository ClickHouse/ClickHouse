-- Tags: zookeeper, no-fasttest, no-replicated-database, no-shared-merge-tree
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: `DatabaseReplicated` does not drop `TimeSeries` inner tables synchronously; deferred DROPs are rejected.
-- Tag no-shared-merge-tree: the test relies on the block-hash insert deduplication of `ReplicatedMergeTree`.

-- The sink writes each samples block to both tables; identical content means a retried block deduplicates in both, so they cannot diverge.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_dedup;

-- The TTL is 10 years: the fixed timestamps below (byte-identical blocks are needed for dedup) must stay inside the TTL window.
CREATE TABLE ts_dedup ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 315360000
SAMPLES INNER ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05025_ts_dedup/samples', 'r1') ORDER BY (id, timestamp)
RECENT SAMPLES ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05025_ts_dedup/recent', 'r1') PARTITION BY toDate(timestamp) ORDER BY (id, timestamp);

SELECT '-- the same block inserted twice is deduplicated in both the samples and the recent samples table';

INSERT INTO ts_dedup (metric_name, tags, time_series) SETTINGS insert_deduplicate = 1 VALUES
    ('dedup_metric', map('env', 'prod'), [(toDateTime64('2026-01-01 00:00:00', 3), 1.), (toDateTime64('2026-01-01 00:00:15', 3), 2.)]);

INSERT INTO ts_dedup (metric_name, tags, time_series) SETTINGS insert_deduplicate = 1 VALUES
    ('dedup_metric', map('env', 'prod'), [(toDateTime64('2026-01-01 00:00:00', 3), 1.), (toDateTime64('2026-01-01 00:00:15', 3), 2.)]);

SELECT
    (SELECT sum(total_rows) FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.samples.%') AS samples_rows,
    (SELECT sum(total_rows) FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%') AS recent_rows;

SELECT '-- a different block is inserted into both';

INSERT INTO ts_dedup (metric_name, tags, time_series) SETTINGS insert_deduplicate = 1 VALUES
    ('dedup_metric', map('env', 'prod'), [(toDateTime64('2026-01-01 00:00:30', 3), 3.)]);

SELECT
    (SELECT sum(total_rows) FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.samples.%') AS samples_rows,
    (SELECT sum(total_rows) FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%') AS recent_rows;

DROP TABLE ts_dedup SYNC;
