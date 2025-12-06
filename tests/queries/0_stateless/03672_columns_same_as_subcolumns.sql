-- https://github.com/ClickHouse/ClickHouse/issues/89599
DROP TABLE IF EXISTS opentelemetry_span_log_9997438610282160742;

CREATE TABLE opentelemetry_span_log_9997438610282160742
(
    `pull_request_number` UInt32,
    `commit_sha` String,
    `check_start_time` DateTime,
    `check_name` LowCardinality(String),
    `instance_type` LowCardinality(String),
    `trace_id` UUID,
    `span_id` UInt64,
    `parent_span_id` UInt64,
    `operation_name` LowCardinality(String),
    `kind` Enum8('INTERNAL' = 0, 'SERVER' = 1, 'CLIENT' = 2, 'PRODUCER' = 3, 'CONSUMER' = 4),
    `start_time_us` UInt64,
    `finish_time_us` UInt64,
    `finish_date` Date,
    `attribute` Map(LowCardinality(String), String),
    `attribute.names` Array(LowCardinality(String)) ALIAS mapKeys(attribute),
    `attribute.values` Array(String) ALIAS mapValues(attribute)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(finish_date)
ORDER BY (check_name, finish_date, finish_time_us, trace_id)
SETTINGS index_granularity = 8192, old_parts_lifetime = 60;

-- attribute.values column may conflict with subcolumn "values" of attribute map
ALTER TABLE opentelemetry_span_log_9997438610282160742 modify setting use_const_adaptive_granularity=1;
ALTER TABLE opentelemetry_span_log_9997438610282160742 rename column span_id to span_id2;

-- attribute.values column (due to it comes first in the definition) will conflict with subcolumn "values" of attribute map
DROP TABLE IF EXISTS opentelemetry_span_log_compact;
CREATE TABLE opentelemetry_span_log_compact
(
    `attribute.names` Array(LowCardinality(String)) ALIAS mapKeys(attribute),
    `attribute.values` Array(String) ALIAS mapValues(attribute),
    `attribute` Map(LowCardinality(String), String)
)
ENGINE = MergeTree
ORDER BY tuple();
