ATTACH TABLE _ UUID '8f34a7a8-1d4f-44d9-87af-3465bb4bd663'
(
    `id` UInt64,
    `j` JSON(max_dynamic_types = 2, max_dynamic_paths = 10)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
