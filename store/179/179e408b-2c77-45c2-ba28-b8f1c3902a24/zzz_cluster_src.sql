ATTACH TABLE _ UUID '0744498e-a7e2-4292-bdc9-251c896071e4'
(
    `id` UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
