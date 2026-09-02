ATTACH TABLE _ UUID 'fa405ffa-5883-49e7-99ca-5a1aff20cdb3'
(
    `id` UInt64,
    `value` String,
    `event_time` DateTime DEFAULT now() - toIntervalDay(2)
)
ENGINE = MergeTree
ORDER BY id
TTL event_time + toIntervalYear(10)
SETTINGS max_number_of_merges_with_ttl_in_pool = 0, merge_with_ttl_timeout = 0, min_bytes_for_wide_part = 1, index_granularity = 8192
