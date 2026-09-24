-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Random settings limits: optimize_read_in_order=(1, 1); max_threads=(8, 8); max_block_size=(1, 1)
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the query checks the local hybrid plan only.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables are incompatible with replicated databases.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
-- This test asserts native plan steps, which cannot use serialized query plans.
SET serialize_query_plan = 0;
-- This test certifies the ordered-read storage-fusion path. The random-settings
-- limits above keep its route-defining settings fixed while all unrelated
-- settings remain randomized by the stateless runner.
SET optimize_read_in_order = 1;
-- Storage fusion splits selected primary-key ranges into at least two ordered
-- layers; `max_threads = 1` intentionally disables that optimization.
SET max_threads = 8;
-- The production policy avoids read lanes smaller than one ordinary block.
-- This tiny fixture uses one row per block so it still exercises that path.
SET max_block_size = 1;
SET enable_promql_native_storage_fusion = 1;

DROP TABLE IF EXISTS promql_native_d06_two_rate_sum;

CREATE TABLE promql_native_d06_two_rate_sum ENGINE = TimeSeries
SETTINGS samples_compression_codec = 'ZSTD(3)', recent_samples_compression_codec = 'ZSTD(3)'
SAMPLES INNER ENGINE = AggregatingMergeTree ORDER BY (id, bucket)
    SETTINGS index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

-- The two metrics have different metric names, so default PromQL vector matching
-- must match them one-to-one on all remaining labels. The first pair includes a
-- counter reset in `reads`; the second pair uses a distinct `instance` label but
-- contributes to the same outer (namespace, pod) group. Read-only and write-only
-- instances have no match and must be dropped before `sum by`.
INSERT INTO promql_native_d06_two_rate_sum (metric_name, tags, samples) VALUES
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
INSERT INTO promql_native_d06_two_rate_sum (metric_name, tags, samples) VALUES
    ('unrelated', map('instance', 'noise', 'namespace', 'prod', 'pod', 'noise'),
        [(toDateTime64(300, 3), 1.), (toDateTime64(360, 3), 2.), (toDateTime64(420, 3), 3.)]);

CREATE TEMPORARY TABLE d06_sql AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_d06_two_rate_sum,
    'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))',
    300, 420, 60)
SETTINGS enable_promql_native_plan = 0;

CREATE TEMPORARY TABLE d06_hybrid AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_d06_two_rate_sum,
    'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))',
    300, 420, 60)
SETTINGS enable_promql_native_plan = 1, enable_promql_native_parallel_processing = 1;

-- `prometheusQueryRange` builds the selector while the table function is being
-- analyzed, before per-query SETTINGS are installed in the outer SELECT.
-- Set raw mode in the session so this test exercises the actual raw selector.
SET enable_promql_native_raw_samples = 1;

CREATE TEMPORARY TABLE d06_raw_hybrid AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_d06_two_rate_sum,
    'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))',
    300, 420, 60)
SETTINGS
    enable_promql_native_plan = 1,
    enable_promql_native_parallel_processing = 1;

SET enable_promql_native_raw_samples = 0;

-- Exercise the serial native two-rate transform with one input stream. This read
-- keeps the storage-owned `Sparse(DateTime64)` bucket column and crosses the
-- transform's input-chunk boundary for the same physical series.
SET max_threads = 1;

CREATE TEMPORARY TABLE d06_serial AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_d06_two_rate_sum,
    'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))',
    300, 420, 60)
SETTINGS enable_promql_native_plan = 1, enable_promql_native_parallel_processing = 1;

SELECT
    countIf(explain LIKE '%PromQLTwoRangeRates%' AND explain NOT LIKE '%MergingTransform%') > 0,
    countIf(explain LIKE '%PromQLTwoRangeRatesMergingTransform%') = 0
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

SELECT count()
FROM
(
    SELECT tags, samples FROM d06_sql
    EXCEPT ALL
    SELECT tags, samples FROM d06_serial
);

SELECT count()
FROM
(
    SELECT tags, samples FROM d06_serial
    EXCEPT ALL
    SELECT tags, samples FROM d06_sql
);

SET max_threads = 8;

-- Both rate branches and their default one-to-one addition must be installed
-- as one fused native plan step. This positive
-- route assertion prevents exact-result equality from hiding a SQL fallback.
SELECT countIf(explain LIKE '%PromQLTwoRangeRatesMergingTransform%') > 0
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

-- Raw-selector mode has a different exact source island: its carrier projection
-- preserves `id`, `bucket`, and the storage-owned `samples` array. Certify that
-- this shape is fused too, rather than silently falling back to the ordinary
-- `PromQLTwoRangeRates` transform.
SET enable_promql_native_raw_samples = 1;

SELECT countIf(explain LIKE '%PromQLTwoRangeRatesMergingTransform%') > 0
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
        enable_promql_native_parallel_processing = 1
);

SET enable_promql_native_raw_samples = 0;

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

-- The independent lane cap is query-scoped; zero chooses lanes from the read
-- budget and selected work, while a positive value remains
-- a hard limit without reducing the read-side `max_threads` budget.
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
    SELECT tags, samples FROM d06_hybrid
    EXCEPT ALL
    SELECT tags, samples FROM d06_sql
);

SELECT count()
FROM
(
    SELECT tags, samples FROM d06_sql
    EXCEPT ALL
    SELECT tags, samples FROM d06_hybrid
);

SELECT count()
FROM
(
    SELECT tags, samples FROM d06_raw_hybrid
    EXCEPT ALL
    SELECT tags, samples FROM d06_sql
);

SELECT count()
FROM
(
    SELECT tags, samples FROM d06_sql
    EXCEPT ALL
    SELECT tags, samples FROM d06_raw_hybrid
);

-- Keep the SQL oracle's exact values visible in the reference output.
SELECT tags, arrayMap(sample -> sample.2, samples)
FROM d06_sql
ORDER BY tags;

SELECT tags, arrayMap(sample -> sample.2, samples)
FROM d06_hybrid
ORDER BY tags;

-- A combined vector-grid budget below the two-branch admission need must fail
-- closed to the SQL plan, while preserving the same exact rows.
CREATE TEMPORARY TABLE d06_fallback AS
SELECT tags, samples
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
    SELECT tags, samples FROM d06_fallback
    EXCEPT ALL
    SELECT tags, samples FROM d06_sql
);

SELECT count()
FROM
(
    SELECT tags, samples FROM d06_sql
    EXCEPT ALL
    SELECT tags, samples FROM d06_fallback
);

SELECT tags, arrayMap(sample -> sample.2, samples)
FROM d06_fallback
ORDER BY tags;

-- The native output-group limit applies to state held by native aggregation
-- steps, not to the intermediate vector-matching keys streamed by this native
-- fragment into its SQL `sum by` parent. Two matched instances and two orphans
-- still produce one valid SQL output group for `pod="api-1"`.
SELECT count(), sum(length(samples))
FROM prometheusQueryRange(
    promql_native_d06_two_rate_sum,
    'ceil(sum by(namespace,pod)(rate(reads{pod="api-1"}[5m])+rate(writes{pod="api-1"}[5m])))',
    300, 420, 60)
SETTINGS
    enable_promql_native_plan = 1,
    enable_promql_native_parallel_processing = 1,
    max_promql_native_output_groups = 1;

SELECT countIf(explain LIKE '%PromQLTwoRangeRates%') > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT *
    FROM prometheusQueryRange(
        promql_native_d06_two_rate_sum,
        'ceil(sum by(namespace,pod)(rate(reads{pod="api-1"}[5m])+rate(writes{pod="api-1"}[5m])))',
        300, 420, 60)
    SETTINGS
        enable_promql_native_plan = 1,
        enable_promql_native_parallel_processing = 1,
        max_promql_native_output_groups = 1
);

-- The SQL parent retains its standard group-by and memory limits. In
-- particular, the native aggregation limit does not cap the two groups that
-- this hybrid query produces after the SQL `sum by`.
SELECT count(), sum(length(samples))
FROM prometheusQueryRange(
    promql_native_d06_two_rate_sum,
    'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))',
    300, 420, 60)
SETTINGS
    enable_promql_native_plan = 1,
    enable_promql_native_parallel_processing = 1,
    max_promql_native_output_groups = 1;

-- Add enough matched physical series for the range splitter to expose all
-- eight read streams. These rows are inserted after the semantic checks above
-- and are used only to certify the lane-selection policy.
INSERT INTO promql_native_d06_two_rate_sum (metric_name, tags, samples)
SELECT
    if(number % 2 = 0, 'reads', 'writes'),
    map(
        'instance', concat('plan-', toString(intDiv(number, 2))),
        'namespace', 'plan',
        'pod', concat('plan-', toString(intDiv(number, 2)))),
    [(toDateTime64(300, 3), toFloat64(number)), (toDateTime64(360, 3), toFloat64(number + 1))]
FROM numbers(16);

-- The fused two-rate automatic policy uses four layers even when the storage
-- read has eight streams. A positive `max_promql_native_parallel_lanes`
-- explicitly overrides that plan-specific default while remaining bounded by
-- the available read streams, selected work, and primary-key split points.
SELECT countIf(explain LIKE '%PromQLTwoRangeRatesMergingTransform%') = 4
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
        max_promql_native_parallel_lanes = 0
);

SELECT countIf(explain LIKE '%PromQLTwoRangeRatesMergingTransform%') > 4
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
        max_promql_native_parallel_lanes = 8
);

DROP TABLE promql_native_d06_two_rate_sum;
