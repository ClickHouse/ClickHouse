-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the test asserts which local query-plan step evaluates PromQL.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables are incompatible with replicated databases.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
-- The distributed-plan shard enables serialization by default. Native PromQL
-- plan steps are deliberately not serializable, so keep this test on its native path.
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS promql_native_range_sum_by;

CREATE TABLE promql_native_range_sum_by ENGINE = TimeSeries
SETTINGS samples_index_granularity = 1;

INSERT INTO promql_native_range_sum_by (metric_name, tags, samples) VALUES
    ('m', map('dc', 'a', 'host', 'h1', 'job', 'api'),
        [(toDateTime64(90, 3), 0.), (toDateTime64(100, 3), 10.), (toDateTime64(110, 3), 30.),
         (toDateTime64(120, 3), 55.), (toDateTime64(130, 3), 85.)]),
    ('m', map('dc', 'a', 'host', 'h2', 'job', 'api'),
        [(toDateTime64(90, 3), 100.), (toDateTime64(100, 3), 120.), (toDateTime64(110, 3), 5.),
         (toDateTime64(120, 3), 25.), (toDateTime64(130, 3), 50.)]),
    ('m', map('dc', 'b', 'host', 'h3', 'job', 'api'),
        [(toDateTime64(95, 3), 0.), (toDateTime64(105, 3), 10.),
         (toDateTime64(125, 3), 30.), (toDateTime64(130, 3), 40.)]),
    ('m', map('dc', 'c', 'host', 'h4', 'job', 'batch'),
        [(toDateTime64(100, 3), 1.), (toDateTime64(130, 3), 31.)]),
    ('n', map('dc', 'a', 'host', 'h5', 'job', 'api'),
        [(toDateTime64(100, 3), 1000.), (toDateTime64(130, 3), 2000.)]);

CREATE TEMPORARY TABLE promql_transpiled AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'sum by (dc) (rate(m{job="api"}[20]))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 0;

CREATE TEMPORARY TABLE promql_native AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'sum by (dc) (rate(m{job="api"}[20]))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 1;

CREATE TEMPORARY TABLE promql_native_parallel AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'sum by (dc) (rate(m{job="api"}[20]))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 1, enable_promql_native_parallel_processing = 1, max_threads = 4;

-- The native path is exactly equivalent to the existing SQL-transpiler path in both directions.
SELECT count()
FROM
(
    SELECT tags, samples FROM promql_native
    EXCEPT ALL
    SELECT tags, samples FROM promql_transpiled
);

SELECT count()
FROM
(
    SELECT tags, samples FROM promql_transpiled
    EXCEPT ALL
    SELECT tags, samples FROM promql_native
);

-- Primary-key range sharding is exactly equivalent to the serial native path.
SELECT count()
FROM
(
    SELECT tags, samples FROM promql_native_parallel
    EXCEPT ALL
    SELECT tags, samples FROM promql_transpiled
);

SELECT count()
FROM
(
    SELECT tags, samples FROM promql_transpiled
    EXCEPT ALL
    SELECT tags, samples FROM promql_native_parallel
);

-- The parallel setting keeps the query on the native step; exact multi-stream
-- merge behavior is covered by the direct `PromQLRangeSumByStep` unit tests.
SELECT countIf(explain LIKE '%PromQLRangeSumBy%') > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT *
    FROM prometheusQueryRange(
        promql_native_range_sum_by,
        'sum by (dc) (rate(m{job="api"}[20]))',
        110, 130, 10)
    SETTINGS enable_promql_native_plan = 1, enable_promql_native_parallel_processing = 1, max_threads = 4
);

-- The selected expression enters the native plan and produces non-empty public query-range rows.
SELECT countIf(explain LIKE '%PromQLRangeSumBy%') > 0
FROM
(
    EXPLAIN
    SELECT *
    FROM prometheusQueryRange(
        promql_native_range_sum_by,
        'sum by (dc) (rate(m{job="api"}[20]))',
        110, 130, 10)
    SETTINGS enable_promql_native_plan = 1
);

SELECT count(), sum(length(samples)) > 0
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'sum by (dc) (rate(m{job="api"}[20]))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 1;

-- The native output cap is an execution contract, not merely a readable setting:
-- both the serial kernel and the parallel final merge must emit at most one row
-- per block when requested.
SELECT max(source_block_size), count()
FROM
(
    SELECT blockSize() AS source_block_size
    FROM prometheusQueryRange(
        promql_native_range_sum_by,
        'sum by (dc) (rate(m{job="api"}[20]))',
        110, 130, 10)
)
SETTINGS enable_promql_native_plan = 1, max_promql_query_block_size = 1;

SELECT max(source_block_size), count()
FROM
(
    SELECT blockSize() AS source_block_size
    FROM prometheusQueryRange(
        promql_native_range_sum_by,
        'sum by (dc) (rate(m{job="api"}[20]))',
        110, 130, 10)
)
SETTINGS
    enable_promql_native_plan = 1,
    enable_promql_native_parallel_processing = 1,
    max_promql_query_block_size = 1,
    max_threads = 4;

-- A supported subtree can be evaluated natively while the ordinary SQL analyzer
-- composes the unsupported parent around its `VECTOR_GRID` output.
CREATE TEMPORARY TABLE promql_clamp_max_transpiled AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'clamp_max(sum by (dc) (rate(m{job="api"}[20])), 2)',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 0;

CREATE TEMPORARY TABLE promql_clamp_max_hybrid AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'clamp_max(sum by (dc) (rate(m{job="api"}[20])), 2)',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 1;

SELECT count()
FROM
(
    SELECT tags, samples FROM promql_clamp_max_hybrid
    EXCEPT ALL
    SELECT tags, samples FROM promql_clamp_max_transpiled
);

SELECT count()
FROM
(
    SELECT tags, samples FROM promql_clamp_max_transpiled
    EXCEPT ALL
    SELECT tags, samples FROM promql_clamp_max_hybrid
);

-- Exactly one native subtree is embedded when enabled; disabling the setting
-- leaves the complete expression on the SQL-transpiler path.
SELECT countIf(explain LIKE '%PromQLRangeSumBy%')
FROM
(
    EXPLAIN
    SELECT *
    FROM prometheusQueryRange(
        promql_native_range_sum_by,
        'clamp_max(sum by (dc) (rate(m{job="api"}[20])), 2)',
        110, 130, 10)
    SETTINGS enable_promql_native_plan = 1
);

SELECT countIf(explain LIKE '%PromQLRangeSumBy%')
FROM
(
    EXPLAIN
    SELECT *
    FROM prometheusQueryRange(
        promql_native_range_sum_by,
        'clamp_max(sum by (dc) (rate(m{job="api"}[20])), 2)',
        110, 130, 10)
    SETTINGS enable_promql_native_plan = 0
);

-- The bounded topk extension is exactly equivalent to the existing SQL-transpiler oracle.
CREATE TEMPORARY TABLE promql_topk_transpiled AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'topk(1, sum by (dc) (rate(m{job="api"}[20])))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 0;

CREATE TEMPORARY TABLE promql_topk_native AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'topk(1, sum by (dc) (rate(m{job="api"}[20])))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 1;

SELECT count()
FROM
(
    SELECT tags, samples FROM promql_topk_native
    EXCEPT ALL
    SELECT tags, samples FROM promql_topk_transpiled
);

SELECT count()
FROM
(
    SELECT tags, samples FROM promql_topk_transpiled
    EXCEPT ALL
    SELECT tags, samples FROM promql_topk_native
);

SELECT countIf(explain LIKE '%PromQLRangeSumBy%') > 0
FROM
(
    EXPLAIN
    SELECT *
    FROM prometheusQueryRange(
        promql_native_range_sum_by,
        'topk(1, sum by (dc) (rate(m{job="api"}[20])))',
        110, 130, 10)
    SETTINGS enable_promql_native_plan = 1
);

SELECT countIf(explain LIKE '%PromQLRangeTopKBy%') = 0
FROM
(
    EXPLAIN
    SELECT *
    FROM prometheusQueryRange(
        promql_native_range_sum_by,
        'topk(1, sum by (dc) (rate(m{job="api"}[20])))',
        110, 130, 10)
    SETTINGS enable_promql_native_plan = 1
);

-- The bottomk extension uses the same exact oracle and plan route.
CREATE TEMPORARY TABLE promql_bottomk_transpiled AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'bottomk(1, sum by (dc) (rate(m{job="api"}[20])))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 0;

CREATE TEMPORARY TABLE promql_bottomk_native AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'bottomk(1, sum by (dc) (rate(m{job="api"}[20])))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 1;

SELECT count()
FROM
(
    SELECT tags, samples FROM promql_bottomk_native
    EXCEPT ALL
    SELECT tags, samples FROM promql_bottomk_transpiled
);

SELECT count()
FROM
(
    SELECT tags, samples FROM promql_bottomk_transpiled
    EXCEPT ALL
    SELECT tags, samples FROM promql_bottomk_native
);

SELECT countIf(explain LIKE '%PromQLRangeSumBy%') > 0
FROM
(
    EXPLAIN
    SELECT *
    FROM prometheusQueryRange(
        promql_native_range_sum_by,
        'bottomk(1, sum by (dc) (rate(m{job="api"}[20])))',
        110, 130, 10)
    SETTINGS enable_promql_native_plan = 1
);

SELECT countIf(explain LIKE '%PromQLRangeTopKBy%') = 0
FROM
(
    EXPLAIN
    SELECT *
    FROM prometheusQueryRange(
        promql_native_range_sum_by,
        'bottomk(1, sum by (dc) (rate(m{job="api"}[20])))',
        110, 130, 10)
    SETTINGS enable_promql_native_plan = 1
);

-- Empty dictionaries are valid and produce no series.
SELECT count()
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'sum by (dc) (rate(missing_metric[20]))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 1;

-- Unsupported expressions remain on the SQL-transpiler path.
SELECT countIf(explain LIKE '%PromQLRangeSumBy%')
FROM
(
    EXPLAIN
    SELECT *
    FROM prometheusQueryRange(
        promql_native_range_sum_by,
        'sum by (dc) (last_over_time(m{job="api"}[20]))',
        110, 130, 10)
    SETTINGS enable_promql_native_plan = 1
);

CREATE TEMPORARY TABLE promql_fallback_reference AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'sum by (dc) (last_over_time(m{job="api"}[20]))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 0;

CREATE TEMPORARY TABLE promql_fallback_enabled AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'sum by (dc) (last_over_time(m{job="api"}[20]))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 1;

SELECT count()
FROM
(
    SELECT tags, samples FROM promql_fallback_enabled
    EXCEPT ALL
    SELECT tags, samples FROM promql_fallback_reference
);

SELECT count()
FROM
(
    SELECT tags, samples FROM promql_fallback_reference
    EXCEPT ALL
    SELECT tags, samples FROM promql_fallback_enabled
);

-- Unsupported topk shapes remain on the SQL-transpiler path and preserve exact fallback results.
SELECT countIf(explain LIKE '%PromQLRangeTopKBy%')
FROM
(
    EXPLAIN
    SELECT *
    FROM prometheusQueryRange(
        promql_native_range_sum_by,
        'topk by (dc) (1, sum by (dc) (rate(m{job="api"}[20])))',
        110, 130, 10)
    SETTINGS enable_promql_native_plan = 1
);

CREATE TEMPORARY TABLE promql_grouped_topk_fallback_reference AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'topk by (dc) (1, sum by (dc) (rate(m{job="api"}[20])))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 0;

CREATE TEMPORARY TABLE promql_grouped_topk_fallback_enabled AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'topk by (dc) (1, sum by (dc) (rate(m{job="api"}[20])))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 1;

SELECT count()
FROM
(
    SELECT tags, samples FROM promql_grouped_topk_fallback_enabled
    EXCEPT ALL
    SELECT tags, samples FROM promql_grouped_topk_fallback_reference
);

SELECT count()
FROM
(
    SELECT tags, samples FROM promql_grouped_topk_fallback_reference
    EXCEPT ALL
    SELECT tags, samples FROM promql_grouped_topk_fallback_enabled
);

-- Dynamic bottomk k is also deliberately unsupported by the native classifier and must fall back.
SELECT countIf(explain LIKE '%PromQLRangeTopKBy%')
FROM
(
    EXPLAIN
    SELECT *
    FROM prometheusQueryRange(
        promql_native_range_sum_by,
        'bottomk(1 + 1, sum by (dc) (rate(m{job="api"}[20])))',
        110, 130, 10)
    SETTINGS enable_promql_native_plan = 1
);

-- Native aggregation state is bounded by an explicit setting.
SELECT count()
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'sum by (dc) (rate(m{job="api"}[20]))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 1, max_promql_native_output_groups = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }

SELECT count()
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'sum by (dc) (rate(m{job="api"}[20]))',
    110, 130, 10)
SETTINGS
    enable_promql_native_plan = 1,
    enable_promql_native_parallel_processing = 1,
    max_threads = 4,
    max_promql_native_output_groups = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }

-- The hybrid SQL parent must propagate an exception from its native fragment.
-- It must not replay the query through the all-SQL path after reading the samples.
SELECT count()
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'topk(1, sum by (dc) (rate(m{job="api"}[20])))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 1, max_promql_native_output_groups = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }

-- Disabling native rate-series admission must also disable the direct
-- `sum by (...)(rate(...))` root, not only leaf and hybrid fragments.
SELECT countIf(explain LIKE '%PromQLRangeSumBy%')
FROM
(
    EXPLAIN PIPELINE
    SELECT *
    FROM prometheusQueryRange(
        promql_native_range_sum_by,
        'sum by (dc) (rate(m{job="api"}[20]))',
        110, 130, 10)
    SETTINGS enable_promql_native_plan = 1, max_promql_native_rate_series = 0
);

-- A positive per-series sample cap is enforced by the sliced direct kernel.
SELECT count()
FROM prometheusQueryRange(
    promql_native_range_sum_by,
    'sum by (dc) (rate(m{job="api"}[20]))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 1, max_promql_native_rate_samples_per_series = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }

DROP TABLE promql_native_range_sum_by;
