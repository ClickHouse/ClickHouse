CREATE TABLE dict (key UInt64, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO dict SELECT number, toString(number) FROM numbers(121);
SELECT count() FROM dict SETTINGS rocksdb_enable_approximate_count = 0;
SELECT count() FROM dict SETTINGS optimize_trivial_count_query = 1, rocksdb_enable_approximate_count = 1;
