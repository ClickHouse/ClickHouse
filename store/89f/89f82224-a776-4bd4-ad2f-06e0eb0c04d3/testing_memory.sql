ATTACH TABLE _ UUID '70e1af7c-23ec-4c4a-9678-05aa31c775e5'
(
    `a` UInt64,
    `b` String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192
