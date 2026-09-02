ATTACH TABLE _ UUID '8c1682ef-b64a-4875-a72d-caa9a009bbca'
(
    `id` UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
