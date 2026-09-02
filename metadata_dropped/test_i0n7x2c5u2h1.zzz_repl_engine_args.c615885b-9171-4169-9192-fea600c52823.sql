ATTACH TABLE _ UUID 'c615885b-9171-4169-9192-fea600c52823'
(
    `x` UInt64
)
ENGINE = ReplicatedMergeTree('/test/dump_schema/04836_client_dump_schema_test_i0n7x2c5u2h1/zzz_repl_engine_args', 'r')
ORDER BY x
SETTINGS index_granularity = 8192
