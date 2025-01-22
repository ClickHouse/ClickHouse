ATTACH TABLE _ UUID '06f65b7d-bd0b-44a1-9362-d9931607b5c9'
(
    `key` UInt64,
    `value` UInt64,
    INDEX value_idx value TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY key
SETTINGS index_granularity = 8192
