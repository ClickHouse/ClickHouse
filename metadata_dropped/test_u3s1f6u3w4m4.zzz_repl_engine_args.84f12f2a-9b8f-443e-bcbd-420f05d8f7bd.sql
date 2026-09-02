ATTACH TABLE _ UUID '84f12f2a-9b8f-443e-bcbd-420f05d8f7bd'
(
    `x` UInt64
)
ENGINE = ReplicatedMergeTree('/04836_client_dump_schema_test_u3s1f6u3w4m4/dump_schema/zzz_repl_engine_args', 'r')
ORDER BY x
SETTINGS index_granularity = 8192
