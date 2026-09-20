-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the query checks the local hybrid plan only.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables are incompatible with replicated databases.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS promql_native_d06_two_rate_sum;

CREATE TABLE promql_native_d06_two_rate_sum ENGINE = TimeSeries
SAMPLES INNER ENGINE = AggregatingMergeTree ORDER BY (id, bucket)
    SETTINGS index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

-- The two metrics have different metric names, so default PromQL vector matching
-- must match them one-to-one on all remaining labels. The first pair includes a
-- counter reset in `reads`; the second pair uses a distinct `instance` label but
-- contributes to the same outer (namespace, pod) group. Read-only and write-only
-- instances have no match and must be dropped before `sum by`.
INSERT INTO promql_native_d06_two_rate_sum (metric_name, tags, time_series) VALUES
    ('reads', map('instance', 'i1', 'namespace', 'prod', 'pod', 'api-1'),
        [(toDateTime64(0, 3), 100.), (toDateTime64(60, 3), 200.), (toDateTime64(120, 3), 300.),
         (toDateTime64(180, 3), 10.), (toDateTime64(240, 3), 110.), (toDateTime64(300, 3), 210.),
         (toDateTime64(360, 3), 310.), (toDateTime64(420, 3), 410.)]),
    ('writes', map('instance', 'i1', 'namespace', 'prod', 'pod', 'api-1'),
        [(toDateTime64(0, 3), 0.), (toDateTime64(60, 3), 60.), (toDateTime64(120, 3), 120.),
         (toDateTime64(180, 3), 180.), (toDateTime64(240, 3), 240.), (toDateTime64(300, 3), 300.),
         (toDateTime64(360, 3), 360.), (toDateTime64(420, 3), 420.)]),
    ('reads', map('instance', 'i2', 'namespace', 'prod', 'pod', 'api-1'),
        [(toDateTime64(0, 3), 0.), (toDateTime64(60, 3), 60.), (toDateTime64(120, 3), 120.),
         (toDateTime64(180, 3), 180.), (toDateTime64(240, 3), 240.), (toDateTime64(300, 3), 300.),
         (toDateTime64(360, 3), 360.), (toDateTime64(420, 3), 420.)]),
    ('writes', map('instance', 'i2', 'namespace', 'prod', 'pod', 'api-1'),
        [(toDateTime64(0, 3), 0.), (toDateTime64(60, 3), 30.), (toDateTime64(120, 3), 60.),
         (toDateTime64(180, 3), 90.), (toDateTime64(240, 3), 120.), (toDateTime64(300, 3), 150.),
         (toDateTime64(360, 3), 180.), (toDateTime64(420, 3), 210.)]),
    ('reads', map('instance', 'orphan-read', 'namespace', 'prod', 'pod', 'api-1'),
        [(toDateTime64(0, 3), 0.), (toDateTime64(60, 3), 60.), (toDateTime64(120, 3), 120.),
         (toDateTime64(180, 3), 180.), (toDateTime64(240, 3), 240.), (toDateTime64(300, 3), 300.),
         (toDateTime64(360, 3), 360.), (toDateTime64(420, 3), 420.)]),
    ('writes', map('instance', 'orphan-write', 'namespace', 'prod', 'pod', 'api-1'),
        [(toDateTime64(0, 3), 0.), (toDateTime64(60, 3), 60.), (toDateTime64(120, 3), 120.),
         (toDateTime64(180, 3), 180.), (toDateTime64(240, 3), 240.), (toDateTime64(300, 3), 300.),
         (toDateTime64(360, 3), 360.), (toDateTime64(420, 3), 420.)]),
    ('reads', map('instance', 'i1', 'namespace', 'prod', 'pod', 'api-2'),
        [(toDateTime64(0, 3), 0.), (toDateTime64(60, 3), 60.), (toDateTime64(120, 3), 120.),
         (toDateTime64(180, 3), 180.), (toDateTime64(240, 3), 240.), (toDateTime64(300, 3), 300.),
         (toDateTime64(360, 3), 360.), (toDateTime64(420, 3), 420.)]),
    ('writes', map('instance', 'i1', 'namespace', 'prod', 'pod', 'api-2'),
        [(toDateTime64(0, 3), 0.), (toDateTime64(60, 3), 12.), (toDateTime64(120, 3), 24.),
         (toDateTime64(180, 3), 36.), (toDateTime64(240, 3), 48.), (toDateTime64(300, 3), 60.),
         (toDateTime64(360, 3), 72.), (toDateTime64(420, 3), 84.)]);

-- Keep an unrelated metric in a separate samples part. The exact two-metric range union must
-- prune this part while the `id IN <set>` remains the exact row-level filter.
INSERT INTO promql_native_d06_two_rate_sum (metric_name, tags, time_series) VALUES
    ('unrelated', map('instance', 'noise', 'namespace', 'prod', 'pod', 'noise'),
        [(toDateTime64(300, 3), 1.), (toDateTime64(360, 3), 2.), (toDateTime64(420, 3), 3.)]);

CREATE TEMPORARY TABLE d06_sql AS
SELECT tags, time_series
FROM prometheusQueryRange(
    promql_native_d06_two_rate_sum,
    'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))',
    300, 420, 60)
SETTINGS enable_promql_native_plan = 0;

CREATE TEMPORARY TABLE d06_hybrid AS
SELECT tags, time_series
FROM prometheusQueryRange(
    promql_native_d06_two_rate_sum,
    'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))',
    300, 420, 60)
SETTINGS enable_promql_native_plan = 1, enable_promql_native_parallel_processing = 1;

-- Both rate branches and their default one-to-one addition must be installed
-- as one fused native plan step. This positive
-- route assertion prevents exact-result equality from hiding a SQL fallback.
SELECT countIf(explain LIKE '%(PromQLTwoRangeRates)%') = 1
FROM
(
    EXPLAIN PIPELINE
    SELECT *
    FROM prometheusQueryRange(
        promql_native_d06_two_rate_sum,
        'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))',
        300, 420, 60)
    SETTINGS enable_promql_native_plan = 1, enable_promql_native_parallel_processing = 1
);

-- The fused selector's canonical `reads|writes` metric union must reach primary-key analysis as
-- two exact id ranges. The large prepared id set stays out of index analysis and in the row-level
-- filter, matching the proven single-metric optimization.
SELECT
    countIf(explain LIKE '%Parts: 1/2%') > 0,
    countIf(explain ILIKE '%Condition:%' AND explain ILIKE '%element set%') = 0
FROM
(
    EXPLAIN indexes = 1
    SELECT *
    FROM prometheusQueryRange(
        promql_native_d06_two_rate_sum,
        'ceil(sum by(namespace,pod)(rate(reads{namespace="prod"}[5m])+rate(writes{namespace="prod"}[5m])))',
        300, 420, 60)
    SETTINGS enable_promql_native_plan = 1, enable_promql_native_parallel_processing = 1
);

-- The independent lane cap is query-scoped; zero preserves automatic
-- sharding, while a positive value can limit native primary-key range lanes
-- without reducing the read-side `max_threads` budget.
SELECT getSetting('max_promql_native_parallel_lanes') = 1
SETTINGS max_promql_native_parallel_lanes = 1;

-- The samples read-block cap is native-selector-local; zero preserves the
-- ordinary `max_block_size`, while a positive value is independently tunable.
SELECT getSetting('max_promql_query_block_size') = 16384
SETTINGS max_promql_query_block_size = 16384;

-- Exact row-multiset equality: this catches matching, default metric-name-dropping
-- one-to-one matching, unmatched-series dropping, reset-aware rate, and the
-- distinct physical series that share one outer aggregation group.
SELECT count()
FROM
(
    SELECT tags, time_series FROM d06_hybrid
    EXCEPT ALL
    SELECT tags, time_series FROM d06_sql
);

SELECT count()
FROM
(
    SELECT tags, time_series FROM d06_sql
    EXCEPT ALL
    SELECT tags, time_series FROM d06_hybrid
);

-- Keep the SQL oracle's exact values visible in the reference output.
SELECT tags, arrayMap(sample -> sample.2, time_series)
FROM d06_sql
ORDER BY tags;

SELECT tags, arrayMap(sample -> sample.2, time_series)
FROM d06_hybrid
ORDER BY tags;

-- A combined vector-grid budget below the two-branch admission need must fail
-- closed to the SQL plan, while preserving the same exact rows.
CREATE TEMPORARY TABLE d06_fallback AS
SELECT tags, time_series
FROM prometheusQueryRange(
    promql_native_d06_two_rate_sum,
    'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))',
    300, 420, 60)
SETTINGS
    enable_promql_native_plan = 1,
    enable_promql_native_parallel_processing = 1,
    max_promql_native_vector_grid_cells = 1;

SELECT countIf(explain LIKE '%PromQLTwoRangeRates%')
FROM
(
    EXPLAIN PIPELINE
    SELECT *
    FROM prometheusQueryRange(
        promql_native_d06_two_rate_sum,
        'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))',
        300, 420, 60)
    SETTINGS
        enable_promql_native_plan = 1,
        enable_promql_native_parallel_processing = 1,
        max_promql_native_vector_grid_cells = 1
);

SELECT count()
FROM
(
    SELECT tags, time_series FROM d06_fallback
    EXCEPT ALL
    SELECT tags, time_series FROM d06_sql
);

SELECT count()
FROM
(
    SELECT tags, time_series FROM d06_sql
    EXCEPT ALL
    SELECT tags, time_series FROM d06_fallback
);

SELECT tags, arrayMap(sample -> sample.2, time_series)
FROM d06_fallback
ORDER BY tags;

DROP TABLE promql_native_d06_two_rate_sum;
