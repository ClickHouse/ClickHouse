-- Tags: no-ordinary-database, use-rocksdb

-- Normal importing, as we only insert 1000 rows, so it should be in memtable
CREATE TABLE IF NOT EXISTS rocksdb_worm (key UInt64, value UInt64) ENGINE = EmbeddedRocksDB() PRIMARY KEY key SETTINGS optimize_for_bulk_insert = 0;
INSERT INTO rocksdb_worm SELECT number, number+1 FROM numbers(1000);
SELECT sum(value) FROM system.rocksdb WHERE database = currentDatabase() AND table = 'rocksdb_worm' AND name = 'no.file.opens'; -- should be 0 because all data is still in memtable
SELECT count() FROM rocksdb_worm;

-- With bulk insertion, there is no memtable, so a small insert should create a new file
ALTER TABLE rocksdb_worm MODIFY SETTING optimize_for_bulk_insert = 1;
TRUNCATE TABLE rocksdb_worm;
INSERT INTO rocksdb_worm SELECT number, number+1 FROM numbers(1000);
SELECT sum(value) FROM system.rocksdb WHERE database = currentDatabase() AND table = 'rocksdb_worm' AND name = 'no.file.opens'; -- should be 1
SELECT count() FROM rocksdb_worm;

-- Testing insert with multiple sinks and fixed block size
TRUNCATE TABLE rocksdb_worm;
ALTER TABLE rocksdb_worm MODIFY SETTING bulk_insert_block_size = 500000;
INSERT INTO rocksdb_worm SELECT number, number+1 FROM numbers_mt(1000000) SETTINGS max_insert_threads = 2, max_block_size = 100000;
SELECT sum(value) FROM system.rocksdb WHERE database = currentDatabase() AND table = 'rocksdb_worm' AND name = 'no.file.opens'; -- should be 2 as max_block_size is set to 500000
SELECT count() FROM rocksdb_worm;

