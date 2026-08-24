-- Tags: no-fasttest, no-replicated-database

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET allow_deprecated_database_ordinary = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts_recent_horizon ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 3600, recent_samples_partition_by = 'toStartOfHour(timestamp)';

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.ts_recent_horizon (metric_name, tags, time_series) VALUES
    ('historical_metric', map(), [(toDateTime64('2000-01-01 00:00:00', 3), 1.)]),
    ('historical_metric', map(), [(toDateTime64('2000-01-01 09:30:00', 3), 2.)]),
    ('historical_metric', map(), [(toDateTime64('2000-01-01 10:00:00', 3), 3.)]);

SELECT engine_full NOT LIKE '% TTL %'
FROM system.tables
WHERE database IN ({CLICKHOUSE_DATABASE_1:String}, currentDatabase()) AND name = '.inner.recentsamples.ts_recent_horizon';

DETACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts_recent_horizon;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.`.inner.recentsamples.ts_recent_horizon`
    MODIFY TTL toDateTime(timestamp) + toIntervalSecond(1)
    SETTINGS materialize_ttl_after_modify = 0;

SELECT engine_full LIKE '% TTL %'
FROM system.tables
WHERE database IN ({CLICKHOUSE_DATABASE_1:String}, currentDatabase()) AND name = '.inner.recentsamples.ts_recent_horizon';

ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts_recent_horizon;

OPTIMIZE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts_recent_horizon FINAL;

SELECT engine_full NOT LIKE '% TTL %'
FROM system.tables
WHERE database IN ({CLICKHOUSE_DATABASE_1:String}, currentDatabase()) AND name = '.inner.recentsamples.ts_recent_horizon';

SELECT countIf(timestamp = toDateTime64('2000-01-01 00:00:00', 3))
FROM {CLICKHOUSE_DATABASE_1:Identifier}.`.inner.recentsamples.ts_recent_horizon`;

SELECT count()
FROM {CLICKHOUSE_DATABASE_1:Identifier}.`.inner.recentsamples.ts_recent_horizon`;

SELECT count()
FROM {CLICKHOUSE_DATABASE_1:Identifier}.`.inner.samples.ts_recent_horizon`;

SELECT
    plan LIKE '%.inner.recentsamples.%',
    plan LIKE '%.inner.samples.%'
FROM
(
    SELECT arrayStringConcat(groupArray(explain), '\n') AS plan
    FROM
    (
        EXPLAIN SELECT *
        FROM prometheusQueryRange(
            {CLICKHOUSE_DATABASE_1:Identifier}.ts_recent_horizon,
            'historical_metric',
            toDateTime64('2000-01-01 09:30:00', 3),
            toDateTime64('2000-01-01 10:00:00', 3),
            1800)
    )
);

SELECT
    plan LIKE '%.inner.recentsamples.%',
    plan LIKE '%.inner.samples.%'
FROM
(
    SELECT arrayStringConcat(groupArray(explain), '\n') AS plan
    FROM
    (
        EXPLAIN SELECT *
        FROM prometheusQueryRange(
            {CLICKHOUSE_DATABASE_1:Identifier}.ts_recent_horizon,
            'historical_metric',
            toDateTime64('2000-01-01 00:00:00', 3),
            toDateTime64('2000-01-01 10:00:00', 3),
            3600)
    )
);

OPTIMIZE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts_recent_horizon FINAL;
SELECT count()
FROM {CLICKHOUSE_DATABASE_1:Identifier}.`.inner.recentsamples.ts_recent_horizon`;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
