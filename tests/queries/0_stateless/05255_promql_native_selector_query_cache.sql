-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: this test must exercise the local native plan.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables are incompatible with replicated databases.

SET allow_experimental_time_series_table = 1;
SET enable_analyzer = 1;
SET session_timezone = 'UTC';
SET serialize_query_plan = 0;

SYSTEM CLEAR QUERY CACHE TAG 'promql_native_selector_query_cache';
DROP TABLE IF EXISTS promql_native_selector_query_cache;

CREATE TABLE promql_native_selector_query_cache ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0;

INSERT INTO promql_native_selector_query_cache (metric_name, tags, samples) VALUES
    ('m', map('dc', 'a', 'job', 'api'),
        [(toDateTime64(100, 3), 0.), (toDateTime64(110, 3), 10.), (toDateTime64(120, 3), 20.)]);

SELECT countIf(explain LIKE '%PromQLRangeSumBy%') > 0
FROM
(
    EXPLAIN
    SELECT *
    FROM prometheusQueryRange(
        promql_native_selector_query_cache,
        'sum by (dc) (rate(m{job="api"}[20]))',
        110, 120, 10)
    SETTINGS enable_promql_native_plan = 1, use_query_cache = 0
);

-- The first query may cache its own result, but its generated tags subquery
-- must execute every time to populate the per-query tags collector.
SELECT count() > 0, sum(length(samples)) > 0
FROM prometheusQueryRange(
    promql_native_selector_query_cache,
    'sum by (dc) (rate(m{job="api"}[20]))',
    110, 120, 10)
SETTINGS
    enable_promql_native_plan = 1,
    use_query_cache = 1,
    query_cache_for_subqueries = 1,
    query_cache_tag = 'promql_native_selector_query_cache';

SELECT count() > 0
FROM system.query_cache
WHERE tag = 'promql_native_selector_query_cache' AND is_subquery = 0 AND stale = 0;

SELECT count() = 0
FROM system.query_cache
WHERE tag = 'promql_native_selector_query_cache'
    AND is_subquery = 1
    AND positionCaseInsensitive(query, 'timeSeriesStoreTags') > 0;

-- LIMIT changes the outer cache key while retaining the generated tags
-- subquery key. A cached tags subquery would leave its collector empty here.
SELECT count() > 0, sum(length(samples)) > 0
FROM prometheusQueryRange(
    promql_native_selector_query_cache,
    'sum by (dc) (rate(m{job="api"}[20]))',
    110, 120, 10)
LIMIT 1
SETTINGS
    enable_promql_native_plan = 1,
    use_query_cache = 1,
    query_cache_for_subqueries = 1,
    query_cache_tag = 'promql_native_selector_query_cache';

SELECT count() = 0
FROM system.query_cache
WHERE tag = 'promql_native_selector_query_cache'
    AND is_subquery = 1
    AND positionCaseInsensitive(query, 'timeSeriesStoreTags') > 0;

SYSTEM CLEAR QUERY CACHE TAG 'promql_native_selector_query_cache';
DROP TABLE promql_native_selector_query_cache SYNC;
