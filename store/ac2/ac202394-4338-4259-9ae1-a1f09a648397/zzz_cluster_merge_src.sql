ATTACH TABLE _ UUID '6b086bf2-e624-435e-830c-05e62a4c26d7'
(
    `id` UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
