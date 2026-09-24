-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the query checks the local native plan.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables are incompatible with replicated databases.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
-- The native admission checks below are bypassed by serialized-plan SQL fallback.
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS promql_native_rate_raw_samples;

-- Keep same-bucket rows from separate INSERTs in distinct level-zero parts.
CREATE TABLE promql_native_rate_raw_samples ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 60, samples_index_granularity = 1, recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = AggregatingMergeTree
SETTINGS
    max_bytes_to_merge_at_max_space_in_pool = 1,
    min_bytes_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 0;

-- Timestamps 0.001 and 40 are the inclusive selector boundaries for the query
-- below. Duplicate lower-bound timestamps and samples immediately outside the
-- boundaries exercise the nested lower/upper-bound search.
INSERT INTO promql_native_rate_raw_samples (metric_name, tags, samples) VALUES
    ('reads', map('instance', 'one'),
        [(toDateTime64(0, 3), 0.),
         (toDateTime64('1970-01-01 00:00:00.001', 3), 1.),
         (toDateTime64('1970-01-01 00:00:00.001', 3), 2.),
         (toDateTime64(20, 3), 20.),
         (toDateTime64(40, 3), 40.),
         (toDateTime64(41, 3), 41.)]);

-- Three still-unmerged physical rows for the same ID and bucket. Their ranges
-- overlap, and timestamps 10, 20, and 40 occur in multiple rows. The samples
-- aggregate keeps the greatest value for each duplicate timestamp.
INSERT INTO promql_native_rate_raw_samples (metric_name, tags, samples) VALUES
    ('reads', map('instance', 'overlap'),
        [(toDateTime64(0, 3), 0.),
         (toDateTime64(10, 3), 10.),
         (toDateTime64(20, 3), 20.),
         (toDateTime64(30, 3), 30.),
         (toDateTime64(40, 3), 40.)]);

INSERT INTO promql_native_rate_raw_samples (metric_name, tags, samples) VALUES
    ('reads', map('instance', 'overlap'),
        [(toDateTime64(10, 3), 12.),
         (toDateTime64(20, 3), 18.),
         (toDateTime64(25, 3), 25.),
         (toDateTime64(40, 3), 39.)]);

INSERT INTO promql_native_rate_raw_samples (metric_name, tags, samples) VALUES
    ('reads', map('instance', 'overlap'),
        [(toDateTime64(10, 3), 11.),
         (toDateTime64(20, 3), 22.),
         (toDateTime64(35, 3), 35.),
         (toDateTime64(40, 3), 41.)]);

-- A second metric with the same non-name labels makes this the supported
-- two-rate hybrid shape. The adversarial reads side still arrives as three
-- separate physical rows before the native fragments are installed.
INSERT INTO promql_native_rate_raw_samples (metric_name, tags, samples) VALUES
    ('writes', map('instance', 'one'),
        [(toDateTime64(0, 3), 0.),
         (toDateTime64(10, 3), 5.),
         (toDateTime64(20, 3), 10.),
         (toDateTime64(30, 3), 15.),
         (toDateTime64(40, 3), 20.)]),
    ('writes', map('instance', 'overlap'),
        [(toDateTime64(0, 3), 0.),
         (toDateTime64(10, 3), 5.),
         (toDateTime64(20, 3), 10.),
         (toDateTime64(30, 3), 15.),
         (toDateTime64(40, 3), 20.)]);

SELECT getSetting('enable_promql_native_raw_samples') = 0;

-- Prove that the adversarial series still has three physical rows rather than
-- relying on timing against a background merge.
SELECT count() = 3
FROM timeSeriesSamples(promql_native_rate_raw_samples)
WHERE id IN
(
    SELECT id
    FROM timeSeriesTags(promql_native_rate_raw_samples)
    WHERE metric_name = 'reads' AND tags['instance'] = 'overlap'
);

CREATE TEMPORARY TABLE raw_rate_sql_oracle AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_rate_raw_samples,
    'ceil(sum by(instance)(rate(reads[20s])+rate(writes[20s])))',
    20, 40, 20)
SETTINGS enable_promql_native_plan = 0;

CREATE TEMPORARY TABLE raw_rate_sliced_native AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_rate_raw_samples,
    'ceil(sum by(instance)(rate(reads[20s])+rate(writes[20s])))',
    20, 40, 20)
SETTINGS
    enable_promql_native_plan = 1,
    enable_promql_native_raw_samples = 0;

CREATE TEMPORARY TABLE raw_rate_raw_native AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_rate_raw_samples,
    'ceil(sum by(instance)(rate(reads[20s])+rate(writes[20s])))',
    20, 40, 20)
SETTINGS
    enable_promql_native_plan = 1,
    enable_promql_native_raw_samples = 1;

-- The transpiled oracle has no native step. Both feature-gate positions must
-- still install exactly one fused native two-rate step rather than silently
-- falling back to the SQL plan.
SELECT countIf(explain LIKE '%(PromQLTwoRangeRates)%')
FROM
(
    EXPLAIN PIPELINE
    SELECT *
    FROM prometheusQueryRange(
        promql_native_rate_raw_samples,
        'ceil(sum by(instance)(rate(reads[20s])+rate(writes[20s])))',
        20, 40, 20)
    SETTINGS enable_promql_native_plan = 0
);

SELECT countIf(explain LIKE '%(PromQLTwoRangeRates)%')
FROM
(
    EXPLAIN PIPELINE
    SELECT *
    FROM prometheusQueryRange(
        promql_native_rate_raw_samples,
        'ceil(sum by(instance)(rate(reads[20s])+rate(writes[20s])))',
        20, 40, 20)
    SETTINGS
        enable_promql_native_plan = 1,
        enable_promql_native_raw_samples = 0
);

SELECT countIf(explain LIKE '%(PromQLTwoRangeRates)%')
FROM
(
    EXPLAIN PIPELINE
    SELECT *
    FROM prometheusQueryRange(
        promql_native_rate_raw_samples,
        'ceil(sum by(instance)(rate(reads[20s])+rate(writes[20s])))',
        20, 40, 20)
    SETTINGS
        enable_promql_native_plan = 1,
        enable_promql_native_raw_samples = 1
);

-- Equality checks below cannot pass vacuously: two series, each with two
-- evaluation points, must be present in the SQL oracle.
SELECT count(), sum(length(samples))
FROM raw_rate_sql_oracle;

SELECT count()
FROM
(
    SELECT tags, samples FROM raw_rate_sliced_native
    EXCEPT ALL
    SELECT tags, samples FROM raw_rate_sql_oracle
);

SELECT count()
FROM
(
    SELECT tags, samples FROM raw_rate_sql_oracle
    EXCEPT ALL
    SELECT tags, samples FROM raw_rate_sliced_native
);

SELECT count()
FROM
(
    SELECT tags, samples FROM raw_rate_raw_native
    EXCEPT ALL
    SELECT tags, samples FROM raw_rate_sql_oracle
);

SELECT count()
FROM
(
    SELECT tags, samples FROM raw_rate_sql_oracle
    EXCEPT ALL
    SELECT tags, samples FROM raw_rate_raw_native
);

-- A standalone `rate` must be admitted as a complete native root, not only as
-- an implementation detail of the fused two-rate fragment.
CREATE TEMPORARY TABLE root_rate_sql_oracle AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_rate_raw_samples,
    'rate(reads[20s])',
    20, 40, 20)
SETTINGS enable_promql_native_plan = 0;

CREATE TEMPORARY TABLE root_rate_sliced_native AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_rate_raw_samples,
    'rate(reads[20s])',
    20, 40, 20)
SETTINGS
    enable_promql_native_plan = 1,
    enable_promql_native_raw_samples = 0;

CREATE TEMPORARY TABLE root_rate_raw_native AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_rate_raw_samples,
    'rate(reads[20s])',
    20, 40, 20)
SETTINGS
    enable_promql_native_plan = 1,
    enable_promql_native_raw_samples = 1;

SELECT countIf(explain LIKE '%(PromQLRangeRate)%')
FROM
(
    EXPLAIN PIPELINE
    SELECT *
    FROM prometheusQueryRange(
        promql_native_rate_raw_samples,
        'rate(reads[20s])',
        20, 40, 20)
    SETTINGS enable_promql_native_plan = 0
);

SELECT countIf(explain LIKE '%(PromQLRangeRate)%')
FROM
(
    EXPLAIN PIPELINE
    SELECT *
    FROM prometheusQueryRange(
        promql_native_rate_raw_samples,
        'rate(reads[20s])',
        20, 40, 20)
    SETTINGS
        enable_promql_native_plan = 1,
        enable_promql_native_raw_samples = 0
);

SELECT countIf(explain LIKE '%(PromQLRangeRate)%')
FROM
(
    EXPLAIN PIPELINE
    SELECT *
    FROM prometheusQueryRange(
        promql_native_rate_raw_samples,
        'rate(reads[20s])',
        20, 40, 20)
    SETTINGS
        enable_promql_native_plan = 1,
        enable_promql_native_raw_samples = 1
);

SELECT count(), sum(length(samples))
FROM root_rate_sql_oracle;

SELECT count()
FROM
(
    SELECT tags, samples FROM root_rate_sliced_native
    EXCEPT ALL
    SELECT tags, samples FROM root_rate_sql_oracle
);

SELECT count()
FROM
(
    SELECT tags, samples FROM root_rate_sql_oracle
    EXCEPT ALL
    SELECT tags, samples FROM root_rate_sliced_native
);

SELECT count()
FROM
(
    SELECT tags, samples FROM root_rate_raw_native
    EXCEPT ALL
    SELECT tags, samples FROM root_rate_sql_oracle
);

SELECT count()
FROM
(
    SELECT tags, samples FROM root_rate_sql_oracle
    EXCEPT ALL
    SELECT tags, samples FROM root_rate_raw_native
);

-- The `bucket` column must be read as a sparse physical column by the serial
-- native `rate` path. One-row blocks also cross the series boundary.
SELECT count() > 0, min(dumpColumnStructure(bucket) LIKE '%Sparse%')
FROM timeSeriesSamples(promql_native_rate_raw_samples)
SETTINGS max_threads = 1, max_block_size = 1;

CREATE TEMPORARY TABLE sparse_serial_root_rate_native AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_rate_raw_samples,
    'rate(reads[20s])',
    20, 40, 20)
SETTINGS
    enable_promql_native_plan = 1,
    enable_promql_native_raw_samples = 1,
    enable_promql_native_parallel_processing = 0,
    max_threads = 1,
    max_block_size = 1;

SELECT countIf(explain LIKE '%(PromQLRangeRate)%')
FROM
(
    EXPLAIN PIPELINE
    SELECT *
    FROM prometheusQueryRange(
        promql_native_rate_raw_samples,
        'rate(reads[20s])',
        20, 40, 20)
    SETTINGS
        enable_promql_native_plan = 1,
        enable_promql_native_raw_samples = 1,
        enable_promql_native_parallel_processing = 0,
        max_threads = 1,
        max_block_size = 1
);

SELECT count(), sum(length(samples))
FROM sparse_serial_root_rate_native;

SELECT count()
FROM
(
    SELECT tags, samples FROM sparse_serial_root_rate_native
    EXCEPT ALL
    SELECT tags, samples FROM root_rate_sql_oracle
);

SELECT count()
FROM
(
    SELECT tags, samples FROM root_rate_sql_oracle
    EXCEPT ALL
    SELECT tags, samples FROM sparse_serial_root_rate_native
);

-- The raw reader must reject an oversized physical row from `samples.size0`
-- before it materializes the nested sample elements. The selected time window
-- contains only three of the five samples retained after same-timestamp
-- deduplication, so the sliced path remains valid at this limit and
-- distinguishes the early raw-row admission guard from the transform's
-- cumulative post-slice limit.
SELECT count()
FROM prometheusQueryRange(
    promql_native_rate_raw_samples,
    'rate(reads{instance="one"}[20s])',
    20, 40, 20)
SETTINGS
    enable_promql_native_plan = 1,
    enable_promql_native_raw_samples = 0,
    max_promql_native_rate_samples_per_series = 4;

SELECT countIf(explain LIKE '%(PromQLRangeRate)%')
FROM
(
    EXPLAIN PIPELINE
    SELECT *
    FROM prometheusQueryRange(
        promql_native_rate_raw_samples,
        'rate(reads{instance="one"}[20s])',
        20, 40, 20)
    SETTINGS
        enable_promql_native_plan = 1,
        enable_promql_native_raw_samples = 1,
        max_promql_native_rate_samples_per_series = 4
);

SELECT count()
FROM prometheusQueryRange(
    promql_native_rate_raw_samples,
    'rate(reads{instance="one"}[20s])',
    20, 40, 20)
SETTINGS
    enable_promql_native_plan = 1,
    enable_promql_native_raw_samples = 1,
    max_promql_native_rate_samples_per_series = 4; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

DROP TABLE promql_native_rate_raw_samples;
