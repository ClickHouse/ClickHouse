-- Tags: no-fasttest, no-old-analyzer, no-parallel-replicas, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-old-analyzer: distributed query plans require the analyzer.
-- Tag no-parallel-replicas: the test checks local plan shape and local plan serialization.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables are incompatible with replicated databases.

SET allow_experimental_time_series_table = 1;
SET enable_analyzer = 1;
SET session_timezone = 'UTC';
SET enable_promql_native_plan = 0;
SET enable_parallel_replicas = 0;
SET distributed_plan_execute_locally = 1;
SET make_distributed_plan = 0;
SET serialize_query_plan = 0;
SET use_query_cache = 0;
SET max_execution_time = 30;
SET max_memory_usage = 1073741824;

DROP TABLE IF EXISTS promql_compensated_sum_admission;
DROP TABLE IF EXISTS promql_compensated_sum_samples;

-- Name the samples table so the plan checks can distinguish genuine shared-argument
-- fusion from independently evaluating the two aggregations and joining them.
CREATE TABLE promql_compensated_sum_samples
(
    id UInt64,
    samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3), value Float64))),
    bucket DateTime64(3),
    min_time SimpleAggregateFunction(min, DateTime64(3)),
    max_time SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree ORDER BY (id, bucket);

CREATE TABLE promql_compensated_sum_admission ENGINE = TimeSeries
SETTINGS id_type = 'UInt64', recent_samples_ttl_seconds = 0
SAMPLES promql_compensated_sum_samples;

INSERT INTO promql_compensated_sum_admission (metric_name, tags, samples) VALUES
    ('m', map('host', 'a'), [(toDateTime64(100, 3), 1.)]),
    ('m', map('host', 'b'), [(toDateTime64(100, 3), 3.)]);

-- Pin the aggregate mode in the session. The serialized controls below use
-- explicit query `SETTINGS`, because `clickhouse-local` resets session serialization.
SET enable_promql_compensated_sum = DEFAULT;
SELECT 'default', toUInt8(getSetting('enable_promql_compensated_sum'));
SELECT 'default_sum',
    countIf(explain LIKE '%sumForEach%') > 0,
    countIf(explain LIKE '%sumPromQLForEach%') = 0,
    countIf(explain LIKE '%ReadFromMergeTree%promql_compensated_sum_samples%') = 1,
    countIf(explain LIKE '%Join%') = 0
FROM (EXPLAIN actions = 1 SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m)', 100));
SELECT 'default_fused_left',
    countIf(explain LIKE '%sumForEach%') > 0,
    countIf(explain LIKE '%sumPromQLForEach%') = 0,
    countIf(explain LIKE '%ReadFromMergeTree%promql_compensated_sum_samples%') = 1,
    countIf(explain LIKE '%Join%') = 0
FROM (EXPLAIN actions = 1 SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m) - max(m)', 100));
SELECT 'default_fused_right',
    countIf(explain LIKE '%sumForEach%') > 0,
    countIf(explain LIKE '%sumPromQLForEach%') = 0,
    countIf(explain LIKE '%ReadFromMergeTree%promql_compensated_sum_samples%') = 1,
    countIf(explain LIKE '%Join%') = 0
FROM (EXPLAIN actions = 1 SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'max(m) - sum(m)', 100));
SELECT 'default_sum_value', value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m)', 100);
SELECT 'default_fused_left_value', value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m) - max(m)', 100);
SELECT 'default_fused_right_value', value FROM prometheusQuery(promql_compensated_sum_admission, 'max(m) - sum(m)', 100);

SET enable_promql_compensated_sum = 0;
SELECT 'off_sum',
    countIf(explain LIKE '%sumForEach%') > 0,
    countIf(explain LIKE '%sumPromQLForEach%') = 0,
    countIf(explain LIKE '%ReadFromMergeTree%promql_compensated_sum_samples%') = 1,
    countIf(explain LIKE '%Join%') = 0
FROM (EXPLAIN actions = 1 SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m)', 100));
SELECT 'off_fused_left',
    countIf(explain LIKE '%sumForEach%') > 0,
    countIf(explain LIKE '%sumPromQLForEach%') = 0,
    countIf(explain LIKE '%ReadFromMergeTree%promql_compensated_sum_samples%') = 1,
    countIf(explain LIKE '%Join%') = 0
FROM (EXPLAIN actions = 1 SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m) - max(m)', 100));
SELECT 'off_fused_right',
    countIf(explain LIKE '%sumForEach%') > 0,
    countIf(explain LIKE '%sumPromQLForEach%') = 0,
    countIf(explain LIKE '%ReadFromMergeTree%promql_compensated_sum_samples%') = 1,
    countIf(explain LIKE '%Join%') = 0
FROM (EXPLAIN actions = 1 SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'max(m) - sum(m)', 100));
SELECT 'off_sum_value', value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m)', 100);
SELECT 'off_fused_left_value', value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m) - max(m)', 100);
SELECT 'off_fused_right_value', value FROM prometheusQuery(promql_compensated_sum_admission, 'max(m) - sum(m)', 100);

SET enable_promql_compensated_sum = 1;
SELECT 'on_sum',
    countIf(explain LIKE '%sumPromQLForEach%') > 0,
    countIf(explain LIKE '%sumForEach%') = 0,
    countIf(explain LIKE '%ReadFromMergeTree%promql_compensated_sum_samples%') = 1,
    countIf(explain LIKE '%Join%') = 0
FROM (EXPLAIN actions = 1 SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m)', 100));
SELECT 'on_fused_left',
    countIf(explain LIKE '%sumPromQLForEach%') > 0,
    countIf(explain LIKE '%sumForEach%') = 0,
    countIf(explain LIKE '%ReadFromMergeTree%promql_compensated_sum_samples%') = 1,
    countIf(explain LIKE '%Join%') = 0
FROM (EXPLAIN actions = 1 SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m) - max(m)', 100));
SELECT 'on_fused_right',
    countIf(explain LIKE '%sumPromQLForEach%') > 0,
    countIf(explain LIKE '%sumForEach%') = 0,
    countIf(explain LIKE '%ReadFromMergeTree%promql_compensated_sum_samples%') = 1,
    countIf(explain LIKE '%Join%') = 0
FROM (EXPLAIN actions = 1 SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'max(m) - sum(m)', 100));
SELECT 'on_sum_value', value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m)', 100);
SELECT 'on_fused_left_value', value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m) - max(m)', 100);
SELECT 'on_fused_right_value', value FROM prometheusQuery(promql_compensated_sum_admission, 'max(m) - sum(m)', 100);

-- Each transport request is independent. Reject emitted compensated `SUM` even
-- when its input would be empty at runtime; a missing metric is not compiler `EMPTY`.
SET make_distributed_plan = 1;
SELECT 'distributed_on_rejects_sum';
SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m) - max(m)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'max(m) - sum(m)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(missing_metric)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SET enable_promql_compensated_sum = 0;
-- These controls check compiler admission, not distributed runtime execution.
-- Require the existing aggregate as well as the absence of compensated `SUM`.
SELECT 'distributed_off_sum',
    countIf(explain LIKE '%sumForEach%') > 0,
    countIf(explain LIKE '%sumPromQLForEach%') = 0
FROM (EXPLAIN actions = 1 SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m)', 100));
SELECT 'distributed_off_fused_left',
    countIf(explain LIKE '%sumForEach%') > 0,
    countIf(explain LIKE '%maxForEach%') > 0,
    countIf(explain LIKE '%sumPromQLForEach%') = 0
FROM (EXPLAIN actions = 1 SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m) - max(m)', 100));
SELECT 'distributed_off_fused_right',
    countIf(explain LIKE '%sumForEach%') > 0,
    countIf(explain LIKE '%maxForEach%') > 0,
    countIf(explain LIKE '%sumPromQLForEach%') = 0
FROM (EXPLAIN actions = 1 SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'max(m) - sum(m)', 100));

SET enable_promql_compensated_sum = 1;
SELECT 'distributed_on_max',
    countIf(explain LIKE '%maxForEach%') > 0,
    countIf(explain LIKE '%sumPromQLForEach%') = 0
FROM (EXPLAIN actions = 1 SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'max(m)', 100));
SELECT 'distributed_on_fused_non_sum',
    countIf(explain LIKE '%maxForEach%') > 0,
    countIf(explain LIKE '%minForEach%') > 0,
    countIf(explain LIKE '%sumPromQLForEach%') = 0
FROM (EXPLAIN actions = 1 SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'max(m) - min(m)', 100));

-- At timestamp 100, `m[1s:15s]` aligns its source range to [105, 90].
-- It is compiler `EMPTY` despite the existing `m` samples, so no `SUM` is emitted.
SELECT 'distributed_on_empty_sum', count() FROM prometheusQuery(promql_compensated_sum_admission, 'sum(last_over_time(m[1s:15s]))', 100);
SELECT 'distributed_on_empty_fused_left', count() FROM prometheusQuery(promql_compensated_sum_admission,
    'sum(last_over_time(m[1s:15s])) - max(last_over_time(m[1s:15s]))', 100);
SELECT 'distributed_on_empty_fused_right', count() FROM prometheusQuery(promql_compensated_sum_admission,
    'max(last_over_time(m[1s:15s])) - sum(last_over_time(m[1s:15s]))', 100);

-- Schema-only queries must not construct an executable compensated `SUM` plan.
DESCRIBE prometheusQuery(promql_compensated_sum_admission, 'sum(m)', 100) FORMAT Null;
DESCRIBE prometheusQuery(promql_compensated_sum_admission, 'sum(m) - max(m)', 100) FORMAT Null;
DESCRIBE prometheusQuery(promql_compensated_sum_admission, 'max(m) - sum(m)', 100) FORMAT Null;
SELECT 'distributed_on_schema_allowed';

SET make_distributed_plan = 0;
SET serialize_query_plan = 1;
SELECT 'serialized_on_rejects_sum';
SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m)', 100) SETTINGS serialize_query_plan = 1; -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m) - max(m)', 100) SETTINGS serialize_query_plan = 1; -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'max(m) - sum(m)', 100) SETTINGS serialize_query_plan = 1; -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(missing_metric)', 100) SETTINGS serialize_query_plan = 1; -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SET enable_promql_compensated_sum = 0;
SELECT 'serialized_off_sum', value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m)', 100) SETTINGS serialize_query_plan = 1;
SELECT 'serialized_off_fused_left', value FROM prometheusQuery(promql_compensated_sum_admission, 'sum(m) - max(m)', 100) SETTINGS serialize_query_plan = 1;
SELECT 'serialized_off_fused_right', value FROM prometheusQuery(promql_compensated_sum_admission, 'max(m) - sum(m)', 100) SETTINGS serialize_query_plan = 1;

SET enable_promql_compensated_sum = 1;
SELECT 'serialized_on_max', value FROM prometheusQuery(promql_compensated_sum_admission, 'max(m)', 100) SETTINGS serialize_query_plan = 1;
SELECT 'serialized_on_fused_non_sum', value FROM prometheusQuery(promql_compensated_sum_admission, 'max(m) - min(m)', 100) SETTINGS serialize_query_plan = 1;
SELECT 'serialized_on_empty_sum', count() FROM prometheusQuery(promql_compensated_sum_admission, 'sum(last_over_time(m[1s:15s]))', 100) SETTINGS serialize_query_plan = 1;
SELECT 'serialized_on_empty_fused_left', count() FROM prometheusQuery(promql_compensated_sum_admission,
    'sum(last_over_time(m[1s:15s])) - max(last_over_time(m[1s:15s]))', 100) SETTINGS serialize_query_plan = 1;
SELECT 'serialized_on_empty_fused_right', count() FROM prometheusQuery(promql_compensated_sum_admission,
    'max(last_over_time(m[1s:15s])) - sum(last_over_time(m[1s:15s]))', 100) SETTINGS serialize_query_plan = 1;
DESCRIBE prometheusQuery(promql_compensated_sum_admission, 'sum(m)', 100) SETTINGS serialize_query_plan = 1 FORMAT Null;
DESCRIBE prometheusQuery(promql_compensated_sum_admission, 'sum(m) - max(m)', 100) SETTINGS serialize_query_plan = 1 FORMAT Null;
DESCRIBE prometheusQuery(promql_compensated_sum_admission, 'max(m) - sum(m)', 100) SETTINGS serialize_query_plan = 1 FORMAT Null;
SELECT 'serialized_on_schema_allowed';

SET serialize_query_plan = 0;
SET enable_promql_compensated_sum = 0;
DROP TABLE promql_compensated_sum_admission SYNC;
DROP TABLE promql_compensated_sum_samples SYNC;
