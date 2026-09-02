ATTACH TABLE _ UUID '5e2caa00-5f38-4350-80b3-105240f76c91'
(
    `x` UInt64
)
ENGINE = ReplicatedMergeTree('/test/dump_schema/04836_client_dump_schema_test_x6z9k9c3h5o6/zzz_repl_engine_args', 'r')
ORDER BY x
SETTINGS index_granularity = 8192
