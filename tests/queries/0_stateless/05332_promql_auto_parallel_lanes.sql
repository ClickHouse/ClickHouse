-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Random settings limits: optimize_read_in_order=(1, 1); max_threads_min_free_memory_per_thread=(0, 0); max_streams_to_max_threads_ratio=(1, 1); max_streams_for_merge_tree_reading=(0, 0); allow_asynchronous_read_from_io_pool_for_merge_tree=(0, 0); serialize_query_plan=(0, 0)
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the test checks local whole-ID range splitting.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables are incompatible with replicated databases.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET serialize_query_plan = 0;
SET optimize_read_in_order = 1;
SET max_threads_min_free_memory_per_thread = 0;
SET max_streams_to_max_threads_ratio = 1;
SET max_streams_for_merge_tree_reading = 0;
SET allow_asynchronous_read_from_io_pool_for_merge_tree = 0;
SET max_block_size = 1;
SET max_promql_query_block_size = 0;
SET enable_promql_native_storage_fusion = 1;
SET enable_promql_native_parallel_processing = 1;
SET enable_promql_native_raw_samples = 1;
SET max_promql_native_parallel_lanes = 0;

DROP TABLE IF EXISTS promql_auto_lanes;
CREATE TABLE promql_auto_lanes ENGINE = TimeSeries
SETTINGS
    id_type = 'UInt64',
    id_generator = 'plus(multiply(toUInt64(tags[\'instance\']), toUInt64(2)), toUInt64(equals(metric_name, \'writes\')))',
    samples_bucket_step_seconds = 3600,
    recent_samples_ttl_seconds = 0,
    samples_compression_codec = 'ZSTD(3)'
SAMPLES INNER ENGINE = AggregatingMergeTree ORDER BY (id, bucket)
    SETTINGS index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

-- One part, 64 distinct physical IDs, one row per mark. There is enough selected
-- work and there are enough whole-ID boundaries for both 8 and 16 layers.
INSERT INTO promql_auto_lanes (metric_name, tags, samples)
SELECT
    arrayJoin(['reads', 'writes']) AS metric_name,
    map('instance', toString(number), 'namespace', 'lanes', 'pod', 'dense'),
    [(toDateTime64(300, 3), 200. * if(number = 0, 0.25, 0.125)),
     (toDateTime64(360, 3), 400. * if(number = 0, 0.25, 0.125))]
FROM numbers(32)
SETTINGS max_threads = 1, max_block_size = 4096, max_insert_block_size = 4096;

SET max_threads = 8;
SET enable_promql_native_plan = 0;
CREATE TEMPORARY TABLE auto_lanes_sql AS
SELECT tags, samples FROM prometheusQueryRange(promql_auto_lanes,
    'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60);

-- Feature OFF is a no-op even with an automatic lane budget.
SELECT countIf(explain LIKE '%PromQL%') = 0 FROM
(
    EXPLAIN PIPELINE
    SELECT * FROM prometheusQueryRange(promql_auto_lanes,
        'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);

-- Pin settings in the session: the table function builds its nested plan before
-- the outer SELECT's SETTINGS are installed.
SET enable_promql_native_plan = 1;
SELECT sum(if(explain LIKE '%PromQLTwoRangeRatesMergingTransform%',
    if(position(explain, '×') = 0, 1, toUInt64(extract(explain, '× ([0-9]+)'))), 0)) = 8 FROM
(
    EXPLAIN PIPELINE
    SELECT * FROM prometheusQueryRange(promql_auto_lanes,
        'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);

CREATE TEMPORARY TABLE auto_lanes_eight AS
SELECT tags, samples FROM prometheusQueryRange(promql_auto_lanes,
    'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60);
SELECT
    (SELECT arraySort(groupArray((tags, samples))) FROM auto_lanes_sql)
    = (SELECT arraySort(groupArray((tags, samples))) FROM auto_lanes_eight);

SET max_threads = 16;
SELECT sum(if(explain LIKE '%PromQLTwoRangeRatesMergingTransform%',
    if(position(explain, '×') = 0, 1, toUInt64(extract(explain, '× ([0-9]+)'))), 0)) = 16 FROM
(
    EXPLAIN PIPELINE
    SELECT * FROM prometheusQueryRange(promql_auto_lanes,
        'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);
SELECT
    (SELECT arraySort(groupArray((tags, samples))) FROM auto_lanes_sql)
    = (SELECT arraySort(groupArray((tags, samples))) FROM prometheusQueryRange(promql_auto_lanes,
        'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60));

-- Read streams can outnumber execution threads. Auto must use the captured
-- execution budget, not 2 * 8 requested read streams or a fixed four-lane cap.
SET max_threads = 2;
SET max_streams_to_max_threads_ratio = 8;
SELECT sum(if(explain LIKE '%PromQLTwoRangeRatesMergingTransform%',
    if(position(explain, '×') = 0, 1, toUInt64(extract(explain, '× ([0-9]+)'))), 0)) = 2 FROM
(
    EXPLAIN PIPELINE
    SELECT * FROM prometheusQueryRange(promql_auto_lanes,
        'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);

-- A positive cap replaces the automatic thread bound but does not grant CPU
-- threads. It remains a ceiling on logical lanes, not a forced width.
SET max_promql_native_parallel_lanes = 8;
SELECT getSetting('max_threads') = 2 AND sum(if(explain LIKE '%PromQLTwoRangeRatesMergingTransform%',
    if(position(explain, '×') = 0, 1, toUInt64(extract(explain, '× ([0-9]+)'))), 0)) = 8 FROM
(
    EXPLAIN PIPELINE
    SELECT * FROM prometheusQueryRange(promql_auto_lanes,
        'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);
SELECT
    (SELECT arraySort(groupArray((tags, samples))) FROM auto_lanes_sql)
    = (SELECT arraySort(groupArray((tags, samples))) FROM prometheusQueryRange(promql_auto_lanes,
        'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60));

SET max_promql_native_parallel_lanes = 1;
SELECT sum(if(explain LIKE '%PromQLTwoRangeRatesMergingTransform%',
    if(position(explain, '×') = 0, 1, toUInt64(extract(explain, '× ([0-9]+)'))), 0)) = 0 FROM
(
    EXPLAIN PIPELINE
    SELECT * FROM prometheusQueryRange(promql_auto_lanes,
        'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);

SET max_threads = 16;
SET max_streams_to_max_threads_ratio = 1;
SET max_promql_native_parallel_lanes = 99;
SELECT sum(if(explain LIKE '%PromQLTwoRangeRatesMergingTransform%',
    if(position(explain, '×') = 0, 1, toUInt64(extract(explain, '× ([0-9]+)'))), 0)) = 16 FROM
(
    EXPLAIN PIPELINE
    SELECT * FROM prometheusQueryRange(promql_auto_lanes,
        'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);

-- Retain the selected-work guard: 64 rows at 32 rows/block admit at most two
-- lanes even though both the thread budget and the manual ceiling are larger.
SET max_block_size = 32;
SELECT sum(if(explain LIKE '%PromQLTwoRangeRatesMergingTransform%',
    if(position(explain, '×') = 0, 1, toUInt64(extract(explain, '× ([0-9]+)'))), 0)) = 2 FROM
(
    EXPLAIN PIPELINE
    SELECT * FROM prometheusQueryRange(promql_auto_lanes,
        'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);
SET max_promql_native_parallel_lanes = 0;
SELECT sum(if(explain LIKE '%PromQLTwoRangeRatesMergingTransform%',
    if(position(explain, '×') = 0, 1, toUInt64(extract(explain, '× ([0-9]+)'))), 0)) = 2 FROM
(
    EXPLAIN PIPELINE
    SELECT * FROM prometheusQueryRange(promql_auto_lanes,
        'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);

-- Many bucket rows but only two physical IDs: actual geometry must win over
-- both the requested streams and the configured ceiling.
DROP TABLE IF EXISTS promql_auto_lanes_sparse;
CREATE TABLE promql_auto_lanes_sparse ENGINE = TimeSeries
SETTINGS
    id_type = 'UInt64',
    id_generator = 'toUInt64(equals(metric_name, \'writes\'))',
    samples_bucket_step_seconds = 1,
    recent_samples_ttl_seconds = 0,
    samples_compression_codec = 'ZSTD(3)'
SAMPLES INNER ENGINE = AggregatingMergeTree ORDER BY (id, bucket)
    SETTINGS index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO promql_auto_lanes_sparse (metric_name, tags, samples)
SELECT
    arrayJoin(['reads', 'writes']) AS metric_name,
    map('instance', 'one', 'namespace', 'lanes', 'pod', 'sparse'),
    arrayMap(k -> (toDateTime64(120 + 3 * k, 3), toFloat64(120 + 3 * k) * 0.125), range(64))
SETTINGS max_threads = 1, max_block_size = 4096, max_insert_block_size = 4096;
SET max_block_size = 1;
SELECT sum(if(explain LIKE '%PromQLTwoRangeRatesMergingTransform%',
    if(position(explain, '×') = 0, 1, toUInt64(extract(explain, '× ([0-9]+)'))), 0)) <= 2 FROM
(
    EXPLAIN PIPELINE
    SELECT * FROM prometheusQueryRange(promql_auto_lanes_sparse,
        'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 309, 309, 60)
);
SET max_promql_native_parallel_lanes = 99;
SELECT sum(if(explain LIKE '%PromQLTwoRangeRatesMergingTransform%',
    if(position(explain, '×') = 0, 1, toUInt64(extract(explain, '× ([0-9]+)'))), 0)) <= 2 FROM
(
    EXPLAIN PIPELINE
    SELECT * FROM prometheusQueryRange(promql_auto_lanes_sparse,
        'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 309, 309, 60)
);

-- Check the full admitted native query rows, including interleaved groups and
-- unmatched inputs. Grouping by `instance` avoids a cross-instance `Float64`
-- reduction at the discontinuity of `ceil`. The direct pair is a SQL-only oracle.
DROP TABLE IF EXISTS promql_auto_lanes_boundary;
CREATE TABLE promql_auto_lanes_boundary ENGINE = TimeSeries
SETTINGS
    id_type = 'UInt64',
    id_generator = 'plus(multiply(toUInt64(tags[\'instance\']), toUInt64(2)), toUInt64(equals(metric_name, \'writes\')))',
    samples_bucket_step_seconds = 3600,
    recent_samples_ttl_seconds = 0,
    samples_compression_codec = 'ZSTD(3)'
SAMPLES INNER ENGINE = AggregatingMergeTree ORDER BY (id, bucket)
    SETTINGS index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO promql_auto_lanes_boundary (metric_name, tags, samples)
WITH multiIf(number = 0, 0.5,
    number = 3, exp2(-53) * if(metric_name = 'reads', 0.75, 0.25),
    number = 5, exp2(-53) * if(metric_name = 'reads', 0.25, 0.75), 0.) AS value
SELECT
    arrayJoin(if(number < 6, ['reads', 'writes'], ['reads'])) AS metric_name,
    map('instance', toString(number), 'namespace', 'fp', 'pod', if(number IN (1, 4, 7), 'two', 'one')),
    [(toDateTime64(300, 3), 200. * value), (toDateTime64(360, 3), 400. * value)]
FROM numbers(9)
SETTINGS max_threads = 1, max_block_size = 4096, max_insert_block_size = 4096;

SET max_threads = 8;
SET max_promql_native_parallel_lanes = 0;
-- The 60-second sample interval extrapolates by 30 seconds at the start of the
-- 300-second range, so each rate is its input `value`. Matched pairs are exactly
-- 1, 0, 0, 2^-53, 0, 2^-53; unmatched reads must not produce output rows.
CREATE TEMPORARY TABLE auto_lanes_boundary_expected AS
SELECT
    arraySort([('instance', toString(number)), ('namespace', 'fp'),
        ('pod', if(number IN (1, 4), 'two', 'one'))]) AS tags,
    [(toDateTime64(360, 3), multiIf(number = 0, toUInt64(4607182418800017408),
        number IN (3, 5), toUInt64(4368491638549381120), toUInt64(0)))] AS samples
FROM numbers(6);
CREATE TEMPORARY TABLE auto_lanes_boundary_ceil_expected AS
SELECT
    arraySort([('instance', toString(number)), ('namespace', 'fp'),
        ('pod', if(number IN (1, 4), 'two', 'one'))]) AS tags,
    [(toDateTime64(360, 3), if(number IN (0, 3, 5),
        toUInt64(4607182418800017408), toUInt64(0)))] AS samples
FROM numbers(6);
SET enable_promql_native_plan = 0;
CREATE TEMPORARY TABLE auto_lanes_boundary_sql AS
SELECT tags, samples FROM prometheusQueryRange(promql_auto_lanes_boundary,
    'rate(reads[5m])+rate(writes[5m])', 360, 360, 60);
SELECT
    (SELECT arraySort(groupArray((tags, samples))) FROM auto_lanes_boundary_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples)))) FROM auto_lanes_boundary_sql);
SET enable_promql_native_plan = 1;
SELECT sum(if(explain LIKE '%PromQLTwoRangeRatesMergingTransform%',
    if(position(explain, '×') = 0, 1, toUInt64(extract(explain, '× ([0-9]+)'))), 0)) = 8 FROM
(
    EXPLAIN PIPELINE
    SELECT * FROM prometheusQueryRange(promql_auto_lanes_boundary,
        'ceil(sum by(namespace,pod,instance)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);
SELECT
    (SELECT arraySort(groupArray((tags, samples))) FROM auto_lanes_boundary_ceil_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_auto_lanes_boundary,
            'ceil(sum by(namespace,pod,instance)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60));
SET max_promql_native_parallel_lanes = 2;
SELECT sum(if(explain LIKE '%PromQLTwoRangeRatesMergingTransform%',
    if(position(explain, '×') = 0, 1, toUInt64(extract(explain, '× ([0-9]+)'))), 0)) = 2 FROM
(
    EXPLAIN PIPELINE
    SELECT * FROM prometheusQueryRange(promql_auto_lanes_boundary,
        'ceil(sum by(namespace,pod,instance)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);
SELECT
    (SELECT arraySort(groupArray((tags, samples))) FROM auto_lanes_boundary_ceil_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_auto_lanes_boundary,
            'ceil(sum by(namespace,pod,instance)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60));

DROP TABLE promql_auto_lanes_boundary;
DROP TABLE promql_auto_lanes_sparse;
DROP TABLE promql_auto_lanes;
