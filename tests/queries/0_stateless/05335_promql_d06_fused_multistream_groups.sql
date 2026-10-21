-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Random settings limits: optimize_read_in_order=(1, 1); max_threads=(8, 8); max_block_size=(1, 1)
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the query asserts the local fused pipeline.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables are incompatible with replicated databases.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET serialize_query_plan = 0;
SET optimize_read_in_order = 1;
SET max_threads = 8;
SET max_block_size = 1;
SET max_promql_query_block_size = 0;
SET enable_promql_native_parallel_processing = 1;
SET enable_promql_native_storage_fusion = 1;
SET max_promql_native_parallel_lanes = 8;
SET enable_promql_native_raw_samples = 0;

DROP TABLE IF EXISTS promql_d06_fused_multistream_groups;

CREATE TABLE promql_d06_fused_multistream_groups ENGINE = TimeSeries
SETTINGS samples_compression_codec = 'ZSTD(3)', recent_samples_compression_codec = 'ZSTD(3)'
SAMPLES INNER ENGINE = AggregatingMergeTree ORDER BY (id, bucket)
    SETTINGS index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

-- Eight matched instances contribute to each of four (namespace, pod) groups.
-- Reuse instance names across groups so matching must preserve all labels.
-- At timestamp `60 * k`, each counter is `10000 + 60 * slope * k + 15 * k * k`.
-- The positive offset prevents zero-point extrapolation. For k = 5, 6, 7,
-- `rate` is `slope + k / 2 - 1`. The group totals before `ceil` are
-- `24 * (group_index + 1) + [28.5, 36.5, 44.5]`, half an integer from either
-- boundary. Floating roundoff and row order may differ; losing, duplicating,
-- or misassigning output groups, values, or timestamps is forbidden.
INSERT INTO promql_d06_fused_multistream_groups (metric_name, tags, samples)
WITH
    intDiv(number, 16) AS group_index,
    intDiv(number, 2) % 8 AS instance_index,
    toFloat64((group_index + 1) * (1 + number % 2))
        + toFloat64(instance_index) / 16 + toFloat64(number % 2) / 8 AS slope
SELECT
    if(number % 2 = 0, 'reads', 'writes'),
    map(
        'instance', concat('i-', toString(instance_index)),
        'namespace', concat('ns-', toString(group_index % 2)),
        'pod', concat('pod-', toString(intDiv(group_index, 2)))),
    arrayMap(k -> (toDateTime64(60 * k, 3), 10000. + 60 * slope * k + 15. * k * k), range(8))
FROM numbers(64);

-- Missing sides share an outer group with matched instances but must be dropped
-- before `sum by`. Large rates make accidental inclusion unambiguous.
INSERT INTO promql_d06_fused_multistream_groups (metric_name, tags, samples)
WITH intDiv(number, 2) AS group_index
SELECT
    if(number % 2 = 0, 'reads', 'writes'),
    map(
        'instance', if(number % 2 = 0, 'orphan-read', 'orphan-write'),
        'namespace', concat('ns-', toString(group_index % 2)),
        'pod', concat('pod-', toString(intDiv(group_index, 2)))),
    arrayMap(k -> (toDateTime64(60 * k, 3), 10000. + 60000. * k), range(8))
FROM numbers(8);

INSERT INTO promql_d06_fused_multistream_groups (metric_name, tags, samples)
SELECT
    'unrelated',
    map(
        'instance', 'noise',
        'namespace', concat('ns-', toString(number % 2)),
        'pod', concat('pod-', toString(intDiv(number, 2)))),
    arrayMap(k -> (toDateTime64(60 * k, 3), 10000. + 60000. * k), range(8))
FROM numbers(4);

CREATE TEMPORARY TABLE d06_groups_expected AS
SELECT
    [('namespace', concat('ns-', toString(number % 2))), ('pod', concat('pod-', toString(intDiv(number, 2))))] AS tags,
    [(toDateTime64(300, 3), toFloat64(53 + 24 * number)),
     (toDateTime64(360, 3), toFloat64(61 + 24 * number)),
     (toDateTime64(420, 3), toFloat64(69 + 24 * number))] AS samples
FROM numbers(4);

CREATE TEMPORARY TABLE d06_groups_sql AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_d06_fused_multistream_groups,
    'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))',
    300, 420, 60)
SETTINGS enable_promql_native_plan = 0;

-- Locate the first aggregation above the fused lane outputs, rather than an
-- unrelated aggregation later in the SQL parent. It must consume multiple
-- streams, and at least two storage-fused rate lanes must be present.
SELECT
    arrayCount(x -> position(x, 'PromQLTwoRangeRatesMergingTransform') > 0, lines) > 1,
    position(
        arrayLast(
            x -> position(x, 'AggregatingTransform') > 0,
            arraySlice(lines, 1, arrayFirstIndex(x -> position(x, 'PromQLTwoRangeRatesMergingTransform') > 0, lines) - 1)),
        'AggregatingTransform × ') > 0
FROM
(
    SELECT groupArray(explain) AS lines
    FROM
    (
        EXPLAIN PIPELINE
        SELECT *
        FROM prometheusQueryRange(
            promql_d06_fused_multistream_groups,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))',
            300, 420, 60)
        SETTINGS enable_promql_native_plan = 1
    )
);

CREATE TEMPORARY TABLE d06_groups_sliced AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_d06_fused_multistream_groups,
    'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))',
    300, 420, 60)
SETTINGS enable_promql_native_plan = 1;

-- `prometheusQueryRange` constructs its selector during table-function analysis.
-- Install raw mode in the session before analyzing both its plan and result.
SET enable_promql_native_raw_samples = 1;

SELECT
    arrayCount(x -> position(x, 'PromQLTwoRangeRatesMergingTransform') > 0, lines) > 1,
    position(
        arrayLast(
            x -> position(x, 'AggregatingTransform') > 0,
            arraySlice(lines, 1, arrayFirstIndex(x -> position(x, 'PromQLTwoRangeRatesMergingTransform') > 0, lines) - 1)),
        'AggregatingTransform × ') > 0
FROM
(
    SELECT groupArray(explain) AS lines
    FROM
    (
        EXPLAIN PIPELINE
        SELECT *
        FROM prometheusQueryRange(
            promql_d06_fused_multistream_groups,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))',
            300, 420, 60)
        SETTINGS enable_promql_native_plan = 1
    )
);

CREATE TEMPORARY TABLE d06_groups_raw AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_d06_fused_multistream_groups,
    'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))',
    300, 420, 60)
SETTINGS enable_promql_native_plan = 1;

-- The SQL oracle must independently match the derived tags, timestamps, and
-- integer results. Bidirectional multisets also reject duplicate output rows.
SELECT count() = 0 FROM
(
    SELECT tags, samples FROM d06_groups_sql
    EXCEPT ALL
    SELECT tags, samples FROM d06_groups_expected
);

SELECT count() = 0 FROM
(
    SELECT tags, samples FROM d06_groups_expected
    EXCEPT ALL
    SELECT tags, samples FROM d06_groups_sql
);

SELECT mode, count() = 4, uniqExact(tags) = 4, sum(length(samples)) = 12
FROM
(
    SELECT 'sql' AS mode, tags, samples FROM d06_groups_sql
    UNION ALL
    SELECT 'sliced' AS mode, tags, samples FROM d06_groups_sliced
    UNION ALL
    SELECT 'raw' AS mode, tags, samples FROM d06_groups_raw
)
GROUP BY mode
ORDER BY mode;

SELECT count() = 0 FROM
(
    SELECT tags, samples FROM d06_groups_sliced
    EXCEPT ALL
    SELECT tags, samples FROM d06_groups_sql
);

SELECT count() = 0 FROM
(
    SELECT tags, samples FROM d06_groups_sql
    EXCEPT ALL
    SELECT tags, samples FROM d06_groups_sliced
);

SELECT count() = 0 FROM
(
    SELECT tags, samples FROM d06_groups_raw
    EXCEPT ALL
    SELECT tags, samples FROM d06_groups_sql
);

SELECT count() = 0 FROM
(
    SELECT tags, samples FROM d06_groups_sql
    EXCEPT ALL
    SELECT tags, samples FROM d06_groups_raw
);

DROP TABLE promql_d06_fused_multistream_groups;
