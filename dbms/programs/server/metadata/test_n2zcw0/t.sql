ATTACH TABLE t
(
    `a` Int, 
    `b` Int
)
ENGINE = MergeTree
ORDER BY (a, b)
SETTINGS index_granularity = 400
