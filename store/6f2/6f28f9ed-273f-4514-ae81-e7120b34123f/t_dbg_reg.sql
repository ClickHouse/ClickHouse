ATTACH TABLE _ UUID 'ecee3e0a-f47a-4c2e-b0a0-477ade441be2'
(
    `id` UInt64,
    `j` JSON(max_dynamic_types = 2, max_dynamic_paths = 10)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
