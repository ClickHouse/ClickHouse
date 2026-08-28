-- Tags: no-fasttest, no-replicated-database
-- PromQL needs ANTLR4, which is disabled in the fast-test build. The TimeSeries table uses external
-- data tables, whose cleanup is not synchronous in DatabaseReplicated.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS promql_timestamp_float32;
DROP TABLE IF EXISTS promql_timestamp_float32_tags;
DROP TABLE IF EXISTS promql_timestamp_float32_samples;

CREATE TABLE promql_timestamp_float32_tags
(
    id UInt64,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time DateTime64(3, 'UTC'),
    max_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree ORDER BY id;

CREATE TABLE promql_timestamp_float32_samples
(
    id UInt64,
    timestamp DateTime64(3, 'UTC'),
    value Float32
)
ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE promql_timestamp_float32
(
    time_series Array(Tuple(DateTime64(3, 'UTC'), Float32))
)
ENGINE = TimeSeries
SAMPLES promql_timestamp_float32_samples
TAGS promql_timestamp_float32_tags;

INSERT INTO promql_timestamp_float32_tags VALUES
    (1, 'float32_timestamp_metric', map(),
     toDateTime64('2025-11-30 10:30:05.125', 3, 'UTC'),
     toDateTime64('2025-11-30 10:30:05.125', 3, 'UTC'));

INSERT INTO promql_timestamp_float32_samples VALUES
    (1, toDateTime64('2025-11-30 10:30:05.125', 3, 'UTC'), 1);

-- A direct selector returns the source sample timestamp, independently of the Float32 sample value type.
SELECT toTypeName(value), value
FROM prometheusQuery(
    'promql_timestamp_float32',
    'timestamp(float32_timestamp_metric)',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

-- A general expression returns the evaluation timestamp and keeps Float64 through common wrappers.
SELECT toTypeName(value), value
FROM prometheusQuery(
    'promql_timestamp_float32',
    'timestamp(vector(1))',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

SELECT toTypeName(value), value
FROM prometheusQuery(
    'promql_timestamp_float32',
    'abs(timestamp(vector(1)))',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

SELECT toTypeName(value), value
FROM prometheusQuery(
    'promql_timestamp_float32',
    'timestamp(vector(1)) + vector(0)',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

-- A scalar-backed range exercises the `timestamp` SCALAR_GRID result and the range table-function schema.
SELECT toTypeName(sample.2), toUnixTimestamp64Milli(sample.1), sample.2
FROM prometheusQueryRange(
    'promql_timestamp_float32',
    'timestamp(vector(1))',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'),
    toDateTime64('2025-11-30 10:30:20.250', 3, 'UTC'),
    5)
ARRAY JOIN time_series AS sample
ORDER BY sample.1;

-- A series-backed range exercises VECTOR_GRID and retains every fractional grid timestamp.
SELECT toTypeName(sample.2), toUnixTimestamp64Milli(sample.1), sample.2
FROM prometheusQueryRange(
    'promql_timestamp_float32',
    'timestamp(float32_timestamp_metric * 1)',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'),
    toDateTime64('2025-11-30 10:30:20.250', 3, 'UTC'),
    5)
ARRAY JOIN time_series AS sample
ORDER BY sample.1;

-- Wrappers that rebuild a scalar-backed piece must materialize it with the carried `value_data_type`
-- (`Float64` for `timestamp`), not with the table's own `Float32` sample type.
SELECT toTypeName(value), value
FROM prometheusQuery(
    'promql_timestamp_float32',
    'label_replace(timestamp(vector(1)), "dst", "x", "", "")',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

SELECT toTypeName(value), value
FROM prometheusQuery(
    'promql_timestamp_float32',
    'label_join(timestamp(vector(1)), "dst", "-", "", "")',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

-- Set operators must not drop the operands' `value_data_type`, otherwise the final cast rounds the timestamp.
SELECT toTypeName(value), value
FROM prometheusQuery(
    'promql_timestamp_float32',
    'timestamp(vector(1)) and vector(1)',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

SELECT toTypeName(value), value
FROM prometheusQuery(
    'promql_timestamp_float32',
    'timestamp(vector(1)) or vector(0)',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

-- `unless`: the left operand keeps its `Float64` values because the right operand has different labels.
SELECT toTypeName(value), value
FROM prometheusQuery(
    'promql_timestamp_float32',
    'label_replace(timestamp(vector(1)), "dst", "x", "", "") unless vector(0)',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

-- An always-empty composition keeps the Float64 override in its schema: clamp() proves emptiness
-- from its constant bounds (max < min) and used to fall back to the table's Float32 value type.
SELECT toTypeName(any(value)), count()
FROM prometheusQuery(
    'promql_timestamp_float32',
    'clamp(timestamp(vector(1)), 2, 1)',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

DROP TABLE promql_timestamp_float32;
DROP TABLE promql_timestamp_float32_tags;
DROP TABLE promql_timestamp_float32_samples;
