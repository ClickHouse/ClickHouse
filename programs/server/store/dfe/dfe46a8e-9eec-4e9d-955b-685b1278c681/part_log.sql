ATTACH TABLE _ UUID '721d6adf-4b1c-49bc-8fbe-1599ac4e048d'
(
    `event_type` Enum8('NewPart' = 1, 'MergeParts' = 2, 'DownloadPart' = 3, 'RemovePart' = 4, 'MutatePart' = 5, 'MovePart' = 6),
    `event_date` Date,
    `event_time` DateTime,
    `duration_ms` UInt64,
    `database` String,
    `table` String,
    `part_name` String,
    `partition_id` String,
    `path_on_disk` String,
    `rows` UInt64,
    `size_in_bytes` UInt64,
    `merged_from` Array(String),
    `bytes_uncompressed` UInt64,
    `read_rows` UInt64,
    `read_bytes` UInt64,
    `peak_memory_usage` UInt64,
    `error` UInt16,
    `exception` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_time)
SETTINGS index_granularity = 8192
