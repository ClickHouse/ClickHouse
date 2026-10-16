-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Random settings limits: optimize_read_in_order=(1, 1); max_threads_min_free_memory_per_thread=(0, 0); max_streams_to_max_threads_ratio=(1, 1); max_streams_for_merge_tree_reading=(0, 0); allow_asynchronous_read_from_io_pool_for_merge_tree=(0, 0); serialize_query_plan=(0, 0); use_query_cache=(0, 0)
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the test checks local whole-ID range splitting.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables are incompatible with replicated databases.

SET allow_experimental_time_series_table = 1;
SET enable_promql_compensated_sum = 1;
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
SET use_query_cache = 0;
SET max_execution_time = 30;
SET max_memory_usage = 1073741824;

-- Pin settings in the session: `prometheusQueryRange` builds its nested plan
-- before the outer `SELECT` installs its `SETTINGS`.
-- Compare the complete public result: both groups, exact tags, timestamp and
-- `Float64` bits. The oracle is not another execution of the query.
CREATE TEMPORARY TABLE promql_compensated_expected AS
SELECT
    arraySort([('namespace', 'fp'), ('pod', if(number = 0, 'one', 'two'))]) AS tags,
    [(toDateTime64(360, 3), if(number = 0, toUInt64(4611686018427387904), toUInt64(0)))] AS samples
FROM numbers(2);

DROP TABLE IF EXISTS promql_compensated_two_rates;
CREATE TABLE promql_compensated_two_rates ENGINE = TimeSeries
SETTINGS
    id_type = 'UInt64',
    id_generator = 'plus(multiply(toUInt64(tags[\'instance\']), toUInt64(2)), toUInt64(equals(metric_name, \'writes\')))',
    samples_bucket_step_seconds = 3600,
    recent_samples_ttl_seconds = 0,
    samples_compression_codec = 'ZSTD(3)'
SAMPLES INNER ENGINE = AggregatingMergeTree ORDER BY (id, bucket)
    SETTINGS index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

-- The original multi-instance fixture, including unmatched reads and
-- interleaved groups. At evaluation 360, the matched pairs contribute
-- `[1, 0, 0, 2^-53, 0, 2^-53]`; group `one` must therefore have `ceil = 2`.
INSERT INTO promql_compensated_two_rates (metric_name, tags, samples)
WITH multiIf(number = 0, 0.5,
    number = 3, exp2(-53) * if(metric_name = 'reads', 0.75, 0.25),
    number = 5, exp2(-53) * if(metric_name = 'reads', 0.25, 0.75), 0.) AS value
SELECT
    arrayJoin(if(number < 6, ['reads', 'writes'], ['reads'])) AS metric_name,
    map('instance', toString(number), 'namespace', 'fp', 'pod', if(number IN (1, 4, 7), 'two', 'one')),
    [(toDateTime64(300, 3), 200. * value), (toDateTime64(360, 3), 400. * value)]
FROM numbers(9)
SETTINGS max_threads = 1, max_block_size = 4096, max_insert_block_size = 4096;

-- The value and plan checks below use the identical original two-rate query.
SET max_threads = 1;
SET enable_promql_native_plan = 0;
SET max_promql_native_parallel_lanes = 0;
SELECT 'two_sql_t1_auto',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60));

SELECT 'plan_two_sql_t1_auto', countIf(explain LIKE '%PromQL%') = 0 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);

SET max_promql_native_parallel_lanes = 8;
SELECT 'two_sql_t1_manual8',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60));

SELECT 'plan_two_sql_t1_manual8', countIf(explain LIKE '%PromQL%') = 0 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);

SET enable_promql_native_plan = 1;
SET max_promql_native_parallel_lanes = 0;
SELECT 'two_native_t1_auto',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60));

SELECT 'plan_two_native_t1_auto', countIf(explain LIKE '%PromQLTwoRangeRates%') > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);

SET max_promql_native_parallel_lanes = 8;
SELECT 'two_native_t1_manual8',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60));

SELECT 'plan_two_native_t1_manual8', countIf(explain LIKE '%PromQLTwoRangeRates%') > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);

-- The value and plan checks below use the identical original two-rate query.
SET max_threads = 2;
SET enable_promql_native_plan = 0;
SET max_promql_native_parallel_lanes = 0;
SELECT 'two_sql_t2_auto',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60));

SELECT 'plan_two_sql_t2_auto', countIf(explain LIKE '%PromQL%') = 0 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);

SET max_promql_native_parallel_lanes = 8;
SELECT 'two_sql_t2_manual8',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60));

SELECT 'plan_two_sql_t2_manual8', countIf(explain LIKE '%PromQL%') = 0 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);

SET enable_promql_native_plan = 1;
SET max_promql_native_parallel_lanes = 0;
SELECT 'two_native_t2_auto',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60));

SELECT 'plan_two_native_t2_auto', countIf(trimLeft(explain) = 'PromQLTwoRangeRatesMergingTransform') = 2 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);

SET max_promql_native_parallel_lanes = 8;
SELECT 'two_native_t2_manual8',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60));

SELECT 'plan_two_native_t2_manual8', countIf(trimLeft(explain) = 'PromQLTwoRangeRatesMergingTransform') = 2 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);

-- The value and plan checks below use the identical original two-rate query.
SET max_threads = 8;
SET enable_promql_native_plan = 0;
SET max_promql_native_parallel_lanes = 0;
SELECT 'two_sql_t8_auto',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60));

SELECT 'plan_two_sql_t8_auto', countIf(explain LIKE '%PromQL%') = 0 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);

SET max_promql_native_parallel_lanes = 8;
SELECT 'two_sql_t8_manual8',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60));

SELECT 'plan_two_sql_t8_manual8', countIf(explain LIKE '%PromQL%') = 0 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);

SET enable_promql_native_plan = 1;
SET max_promql_native_parallel_lanes = 0;
SELECT 'two_native_t8_auto',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60));

SELECT 'plan_two_native_t8_auto', countIf(trimLeft(explain) = 'PromQLTwoRangeRatesMergingTransform') = 8 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);

SET max_promql_native_parallel_lanes = 8;
SELECT 'two_native_t8_manual8',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60));

SELECT 'plan_two_native_t8_manual8', countIf(trimLeft(explain) = 'PromQLTwoRangeRatesMergingTransform') = 8 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(sample.2)), samples))))
        FROM prometheusQueryRange(promql_compensated_two_rates,
            'ceil(sum by(namespace,pod)(rate(reads[5m])+rate(writes[5m])))', 360, 360, 60)
);

-- A separate single-rate fixture exercises the private native partial-state
-- carrier. Its physical-ID order interleaves groups over `[1, 0, e, 0, e]`,
-- where `e = 2^-53`. Both public outputs have the same exact-bit oracle above.
DROP TABLE IF EXISTS promql_compensated_single_rate;
CREATE TABLE promql_compensated_single_rate ENGINE = TimeSeries
SETTINGS
    id_type = 'UInt64',
    id_generator = 'plus(multiply(toUInt64(tags[\'instance\']), toUInt64(2)), toUInt64(equals(metric_name, \'writes\')))',
    samples_bucket_step_seconds = 3600,
    recent_samples_ttl_seconds = 0,
    samples_compression_codec = 'ZSTD(3)'
SAMPLES INNER ENGINE = AggregatingMergeTree ORDER BY (id, bucket)
    SETTINGS index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO promql_compensated_single_rate (metric_name, tags, samples)
WITH multiIf(number = 0, 1., number IN (2, 4), exp2(-53), 0.) AS value
SELECT
    'reads' AS metric_name,
    map('instance', toString(number), 'namespace', 'fp', 'pod', if(number IN (1, 3), 'two', 'one')),
    [(toDateTime64(300, 3), 200. * value), (toDateTime64(360, 3), 400. * value)]
FROM numbers(5)
SETTINGS max_threads = 1, max_block_size = 4096, max_insert_block_size = 4096;

SET max_threads = 1;
SET max_promql_native_parallel_lanes = 0;
SET enable_promql_native_plan = 0;
SELECT 'single_sql_t1_auto',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(ceil(sample.2))), samples))))
        FROM prometheusQueryRange(promql_compensated_single_rate,
            'sum by(namespace,pod)(rate(reads[5m]))', 360, 360, 60));

SELECT 'plan_single_sql_t1_auto', countIf(explain LIKE '%PromQL%') = 0 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(ceil(sample.2))), samples))))
        FROM prometheusQueryRange(promql_compensated_single_rate,
            'sum by(namespace,pod)(rate(reads[5m]))', 360, 360, 60)
);

SET enable_promql_native_plan = 1;
SET max_threads = 1;
SET max_promql_native_parallel_lanes = 0;
SELECT 'single_native_t1_auto',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(ceil(sample.2))), samples))))
        FROM prometheusQueryRange(promql_compensated_single_rate,
            'sum by(namespace,pod)(rate(reads[5m]))', 360, 360, 60));

SELECT 'plan_single_native_t1_auto', countIf(explain LIKE '%PromQLRangeSumBy%') > 0
    AND countIf(explain LIKE '%PromQLPartialGroupMerge%') = 0 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(ceil(sample.2))), samples))))
        FROM prometheusQueryRange(promql_compensated_single_rate,
            'sum by(namespace,pod)(rate(reads[5m]))', 360, 360, 60)
);

SET max_promql_native_parallel_lanes = 8;
SELECT 'single_native_t1_manual8',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(ceil(sample.2))), samples))))
        FROM prometheusQueryRange(promql_compensated_single_rate,
            'sum by(namespace,pod)(rate(reads[5m]))', 360, 360, 60));

SELECT 'plan_single_native_t1_manual8', countIf(explain LIKE '%PromQLRangeSumBy%') > 0
    AND countIf(explain LIKE '%PromQLPartialGroupMerge%') = 0 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(ceil(sample.2))), samples))))
        FROM prometheusQueryRange(promql_compensated_single_rate,
            'sum by(namespace,pod)(rate(reads[5m]))', 360, 360, 60)
);

SET max_threads = 2;
SET max_promql_native_parallel_lanes = 0;
SELECT 'single_native_t2_auto',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(ceil(sample.2))), samples))))
        FROM prometheusQueryRange(promql_compensated_single_rate,
            'sum by(namespace,pod)(rate(reads[5m]))', 360, 360, 60));

SELECT 'plan_single_native_t2_auto', countIf(explain LIKE '%PromQLRangeSumBy ×%') > 0
    AND countIf(explain LIKE '%PromQLPartialGroupMerge%') > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(ceil(sample.2))), samples))))
        FROM prometheusQueryRange(promql_compensated_single_rate,
            'sum by(namespace,pod)(rate(reads[5m]))', 360, 360, 60)
);

SET max_promql_native_parallel_lanes = 8;
SELECT 'single_native_t2_manual8',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(ceil(sample.2))), samples))))
        FROM prometheusQueryRange(promql_compensated_single_rate,
            'sum by(namespace,pod)(rate(reads[5m]))', 360, 360, 60));

SELECT 'plan_single_native_t2_manual8', countIf(explain LIKE '%PromQLRangeSumBy ×%') > 0
    AND countIf(explain LIKE '%PromQLPartialGroupMerge%') > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(ceil(sample.2))), samples))))
        FROM prometheusQueryRange(promql_compensated_single_rate,
            'sum by(namespace,pod)(rate(reads[5m]))', 360, 360, 60)
);

SET max_threads = 8;
SET max_promql_native_parallel_lanes = 0;
SELECT 'single_native_t8_auto',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(ceil(sample.2))), samples))))
        FROM prometheusQueryRange(promql_compensated_single_rate,
            'sum by(namespace,pod)(rate(reads[5m]))', 360, 360, 60));

SELECT 'plan_single_native_t8_auto', countIf(explain LIKE '%PromQLRangeSumBy ×%') > 0
    AND countIf(explain LIKE '%PromQLPartialGroupMerge%') > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(ceil(sample.2))), samples))))
        FROM prometheusQueryRange(promql_compensated_single_rate,
            'sum by(namespace,pod)(rate(reads[5m]))', 360, 360, 60)
);

SET max_promql_native_parallel_lanes = 8;
SELECT 'single_native_t8_manual8',
    (SELECT arraySort(groupArray((tags, samples))) FROM promql_compensated_expected)
    = (SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(ceil(sample.2))), samples))))
        FROM prometheusQueryRange(promql_compensated_single_rate,
            'sum by(namespace,pod)(rate(reads[5m]))', 360, 360, 60));

SELECT 'plan_single_native_t8_manual8', countIf(explain LIKE '%PromQLRangeSumBy ×%') > 0
    AND countIf(explain LIKE '%PromQLPartialGroupMerge%') > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT arraySort(groupArray((arraySort(tags),
        arrayMap(sample -> (sample.1, reinterpretAsUInt64(ceil(sample.2))), samples))))
        FROM prometheusQueryRange(promql_compensated_single_rate,
            'sum by(namespace,pod)(rate(reads[5m]))', 360, 360, 60)
);

DROP TABLE promql_compensated_single_rate;
DROP TABLE promql_compensated_two_rates;
DROP TABLE promql_compensated_expected;
