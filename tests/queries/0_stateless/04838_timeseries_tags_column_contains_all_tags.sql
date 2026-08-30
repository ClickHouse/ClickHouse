-- Tags: no-fasttest
-- ^^ ANTLR4 support is disabled in the fast-test build, and the PromQL
-- grammar requires it.

-- The `tags` column of the tags target table contains all the tags, including the metric name
-- (the `__name__` tag) and the tags with dedicated columns from the `tags_to_columns` setting.
-- The default id generator calculates identifiers from the stored columns `metric_name` and `tags`.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;

CREATE TABLE ts ENGINE = TimeSeries SETTINGS tags_to_columns = {'job': 'job_column', 'instance': 'instance_column'};

SELECT 'inner tags table columns:';
SELECT extract(create_table_query, 'TAGS INNER COLUMNS \((.*?)\) TAGS INNER ENGINE')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts';

INSERT INTO ts (metric_name, tags, time_series) VALUES
    ('http_requests', {'job': 'crawler', 'instance': 'host1:8080', 'region': 'eu'}, [(toDateTime64(1000, 3), 1.5)]);

-- The metric name can also be specified as the `__name__` tag.
INSERT INTO ts (tags, time_series) VALUES
    ({'__name__': 'http_requests', 'job': 'crawler', 'instance': 'host2:8080'}, [(toDateTime64(1060, 3), 2.5)]);

-- The database is passed to `timeSeriesTags` explicitly, otherwise the queries fail
-- with parallel replicas: `timeSeriesTags` reads the inner MergeTree table directly, so the query
-- can be sent to another replica where the current database is different.
SELECT 'stored tags:';
SELECT metric_name, job_column, instance_column, tags FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts') ORDER BY tags;

SELECT 'id is calculated from the stored columns:';
SELECT countIf(id = tuple(sipHash64(metric_name), reinterpretAsUUID(sipHash128(tags)))), count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts');

SELECT 'prometheusQuery:';
SELECT tags, value FROM prometheusQuery(ts, 'http_requests', 1080) ORDER BY tags;

-- For compatibility an id-generator expression can reference `all_tags` - there is no such column,
-- but on insertion it's resolved as the same data as the `tags` column.
ALTER TABLE ts MODIFY SETTING id_generator = 'tuple(sipHash64(metric_name), reinterpretAsUUID(sipHash128(metric_name, all_tags)))';

INSERT INTO ts (metric_name, tags, time_series) VALUES
    ('http_requests', {'job': 'miner', 'instance': 'host3:8080'}, [(toDateTime64(1120, 3), 3.5)]);

SELECT 'id is calculated by the altered id_generator:';
SELECT countIf(id = tuple(sipHash64(metric_name), reinterpretAsUUID(sipHash128(metric_name, tags)))), count()
FROM timeSeriesTags({CLICKHOUSE_DATABASE:String}, 'ts') WHERE tags['job'] = 'miner';

-- The time series inserted before the ALTER are still readable together with the new one.
SELECT 'prometheusQuery after ALTER:';
SELECT tags, value FROM prometheusQuery(ts, 'http_requests', 1140) ORDER BY tags;

DROP TABLE ts;
