ATTACH TABLE _ UUID '59932159-f17d-4ed9-a613-b7da57984e47'
(
    `x` UInt64
)
ENGINE = ReplicatedMergeTree('/test/dump_schema/04836_client_dump_schema_test_y1x5s5d2d1a9/zzz_repl_engine_args', 'r')
ORDER BY x
SETTINGS index_granularity = 8192
