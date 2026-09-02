ATTACH TABLE _ UUID '45b133f8-09bd-486e-b0fb-dd67352cc4f3'
(
    `id` UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
