-- Tags: no-fasttest, no-old-analyzer, no-parallel-replicas, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-old-analyzer: distributed query plans require the analyzer.
-- Tag no-parallel-replicas: this test exercises local distributed-plan serialization only.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables are incompatible with replicated databases.

SET allow_experimental_time_series_table = 1;
SET enable_analyzer = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS promql_native_duplicate_tags;
DROP TABLE IF EXISTS promql_native_serialization;

-- Two physical identifiers can represent one logical series after `id_generator`
-- changes. The SQL plan merges that history by its full tag set; the native
-- streaming kernel cannot, so admission must fail closed to SQL.
CREATE TABLE promql_native_duplicate_tags
ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0
TAGS INNER COLUMNS
(
    id Tuple(UInt64, UUID)
        DEFAULT tuple(sipHash64(tags), reinterpretAsUUID(sipHash128(metric_name, tags)))
);

INSERT INTO promql_native_duplicate_tags (metric_name, tags, samples) VALUES
    ('m', map('dc', 'a', 'job', 'api'),
        [(toDateTime64(90, 3), 0.), (toDateTime64(100, 3), 10.)]);

ALTER TABLE promql_native_duplicate_tags MODIFY SETTING
    id_generator = 'tuple(sipHash64(metric_name), reinterpretAsUUID(sipHash128(tags)))';

INSERT INTO promql_native_duplicate_tags (metric_name, tags, samples) VALUES
    ('m', map('dc', 'a', 'job', 'api'),
        [(toDateTime64(110, 3), 30.), (toDateTime64(120, 3), 60.)]);

CREATE TEMPORARY TABLE duplicate_tags_sql AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_duplicate_tags,
    'sum by (dc) (rate(m{job="api"}[20]))',
    100, 120, 10)
SETTINGS enable_promql_native_plan = 0;

CREATE TEMPORARY TABLE duplicate_tags_fallback AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_duplicate_tags,
    'sum by (dc) (rate(m{job="api"}[20]))',
    100, 120, 10)
SETTINGS enable_promql_native_plan = 1;

SELECT count() FROM
(
    SELECT tags, samples FROM duplicate_tags_fallback
    EXCEPT ALL
    SELECT tags, samples FROM duplicate_tags_sql
);

SELECT count() FROM
(
    SELECT tags, samples FROM duplicate_tags_sql
    EXCEPT ALL
    SELECT tags, samples FROM duplicate_tags_fallback
);

SELECT countIf(explain LIKE '%PromQLRangeSumBy%')
FROM
(
    EXPLAIN
    SELECT *
    FROM prometheusQueryRange(
        promql_native_duplicate_tags,
        'sum by (dc) (rate(m{job="api"}[20]))',
        100, 120, 10)
    SETTINGS enable_promql_native_plan = 1
);

SELECT countIf(explain LIKE '%PromQLRangeSumBy%')
FROM
(
    EXPLAIN
    SELECT *
    FROM prometheusQueryRange(
        promql_native_duplicate_tags,
        'clamp_max(sum by (dc) (rate(m{job="api"}[20])), 10)',
        100, 120, 10)
    SETTINGS enable_promql_native_plan = 1
);

-- Native plan steps do not yet implement query-plan serialization. Each
-- serialization setting must independently keep both direct and hybrid queries
-- on the ordinary SQL plan.
CREATE TABLE promql_native_serialization
ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0;

INSERT INTO promql_native_serialization (metric_name, tags, samples) VALUES
    ('m', map('dc', 'a', 'job', 'api'),
        [(toDateTime64(90, 3), 0.), (toDateTime64(100, 3), 10.),
         (toDateTime64(110, 3), 30.), (toDateTime64(120, 3), 60.)]);

SELECT countIf(explain LIKE '%PromQLRangeSumBy%')
FROM
(
    EXPLAIN
    SELECT *
    FROM prometheusQueryRange(
        promql_native_serialization,
        'sum by (dc) (rate(m{job="api"}[20]))',
        100, 120, 10)
    SETTINGS
        enable_promql_native_plan = 1,
        make_distributed_plan = 1,
        distributed_plan_execute_locally = 1,
        enable_parallel_replicas = 0
);

SELECT countIf(explain LIKE '%PromQLRangeSumBy%')
FROM
(
    EXPLAIN
    SELECT *
    FROM prometheusQueryRange(
        promql_native_serialization,
        'clamp_max(sum by (dc) (rate(m{job="api"}[20])), 10)',
        100, 120, 10)
    SETTINGS
        enable_promql_native_plan = 1,
        serialize_query_plan = 1
);

DROP TABLE promql_native_duplicate_tags SYNC;
DROP TABLE promql_native_serialization SYNC;
