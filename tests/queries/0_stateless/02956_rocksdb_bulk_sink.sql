-- Tags: no-ordinary-database, use-rocksdb
CREATE TABLE IF NOT EXISTS rocksdb_worm (key UInt64, value UInt64) ENGINE = EmbeddedRocksDB() PRIMARY KEY key SETTINGS optimize_for_bulk_insert = 1;
INSERT INTO rocksdb_worm SELECT number, number+1 FROM numbers(1000);
SELECT sum(value) FROM system.rocksdb WHERE database = currentDatabase() AND table = 'rocksdb_worm' AND name = 'no.file.opens'; -- should be 1
SELECT count() FROM rocksdb_worm;
TRUNCATE TABLE rocksdb_worm;
ALTER TABLE rocksdb_worm MODIFY SETTING optimize_for_bulk_insert = 0;
INSERT INTO rocksdb_worm SELECT number, number+1 FROM numbers(1000);
SELECT sum(value) FROM system.rocksdb WHERE database = currentDatabase() AND table = 'rocksdb_worm' AND name = 'no.file.opens'; -- should be 0 because all data is still in memtable
SELECT count() FROM rocksdb_worm;
