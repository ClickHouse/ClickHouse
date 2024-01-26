-- Tags: no-fasttest
CREATE TABLE rocksdb_worm (key UInt64, value UInt64) ENGINE = EmbeddedRocksDB() PRIMARY KEY key SETTINGS optimize_for_bulk_insert = 1;
INSERT INTO rocksdb_worm SELECT number, number+1 FROM numbers_mt(1000) SETTINGS max_insert_threads = 2;
SELECT count() FROM rocksdb_worm;
