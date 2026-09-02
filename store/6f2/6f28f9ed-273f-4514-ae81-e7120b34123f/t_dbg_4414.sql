ATTACH TABLE _ UUID 'c2905d1f-d8b0-42be-b44b-6c5bf7d59d7f'
(
    `id` UInt64,
    `j` JSON(max_dynamic_types = 2, max_dynamic_paths = 10)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
