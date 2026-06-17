-- Tags: zookeeper, no-ordinary-database, use-rocksdb

DROP TABLE IF EXISTS 02526_keeper_map;
DROP TABLE IF EXISTS 02526_rocksdb;

CREATE TABLE 02526_keeper_map (`key` String, `value` UInt32) ENGINE = KeeperMap('/' || currentDatabase() || '/02526_kv_filter_types') PRIMARY KEY key;
INSERT INTO 02526_keeper_map SELECT * FROM generateRandom('`key` String, `value` UInt32') LIMIT 100;
SELECT * FROM 02526_keeper_map WHERE key in (SELECT number * 5 FROM numbers(1000)) FORMAT Null;

DROP TABLE 02526_keeper_map;

CREATE TABLE 02526_rocksdb (`key` String, `value` UInt32) ENGINE = EmbeddedRocksDB PRIMARY KEY key;
INSERT INTO 02526_rocksdb SELECT * FROM generateRandom('`key` String, `value` UInt32') LIMIT 100;
SELECT * FROM 02526_rocksdb WHERE key in (SELECT number * 5 FROM numbers(1000)) FORMAT Null;

DROP TABLE 02526_rocksdb;
