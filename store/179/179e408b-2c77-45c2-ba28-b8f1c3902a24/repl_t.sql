ATTACH TABLE _ UUID '7870916a-60a7-4829-8186-9bad28758078'
(
    `x` UInt64
)
ENGINE = ReplicatedMergeTree('/test/v114098c/repl_t', 'r1')
ORDER BY x
SETTINGS index_granularity = 8192
