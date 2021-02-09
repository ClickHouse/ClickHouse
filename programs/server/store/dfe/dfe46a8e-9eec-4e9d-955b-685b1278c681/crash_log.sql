ATTACH TABLE _ UUID '5ec3f88a-cd1b-467a-9769-7ff65a09c665'
(
    `event_date` Date,
    `event_time` DateTime,
    `timestamp_ns` UInt64,
    `signal` Int32,
    `thread_id` UInt64,
    `query_id` String,
    `trace` Array(UInt64),
    `trace_full` Array(String),
    `version` String,
    `revision` UInt32,
    `build_id` String
)
ENGINE = MergeTree
ORDER BY (event_date, event_time)
SETTINGS index_granularity = 8192
