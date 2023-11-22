CREATE TABLE t_r1
(
    `id` UInt64,
    `val` SimpleAggregateFunction(max, Nullable(String))
)
ENGINE = ReplicatedAggregatingMergeTree('/tables/{database}/t', 'r1')
ORDER BY id
SETTINGS index_granularity = 8192;

CREATE TABLE t_r2
(
    `id` UInt64,
    `val` SimpleAggregateFunction(anyLast, Nullable(String))
)
ENGINE = ReplicatedAggregatingMergeTree('/tables/{database}/t', 'r2')
ORDER BY id
SETTINGS index_granularity = 8192; -- { serverError INCOMPATIBLE_COLUMNS }
