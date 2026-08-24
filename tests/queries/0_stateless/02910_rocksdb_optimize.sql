-- Tags: use-rocksdb

CREATE TABLE dict (key UInt64, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO dict SELECT number, toString(number) FROM numbers(1e3);
OPTIMIZE TABLE dict;
