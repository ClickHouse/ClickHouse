ATTACH TABLE _ UUID '53bbcc39-63b0-44b6-b31b-4e435c356989'
(
    `d` Date DEFAULT '2000-01-01',
    `x` UInt64,
    `y` UInt64,
    `z` UInt64
)
ENGINE = MergeTree(d, (x, y, z), 1)
