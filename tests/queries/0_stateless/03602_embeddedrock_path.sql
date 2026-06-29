-- Tags: no-fasttest, use-rocksdb
-- no-fasttest: EmbeddedRocksDB requires libraries
CREATE TABLE embeddedrock_exploit
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = EmbeddedRocksDB(150,'/tmp/exploit')
PRIMARY KEY key; -- { serverError BAD_ARGUMENTS }