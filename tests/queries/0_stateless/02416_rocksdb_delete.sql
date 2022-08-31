-- Tags: no-ordinary-database, no-fasttest

DROP TABLE IF EXISTS 02416_rocksdb_delete;

CREATE TABLE 02416_rocksdb_delete (key UInt64, value String) Engine=EmbeddedRocksDB PRIMARY KEY(key);

INSERT INTO 02416_rocksdb_delete VALUES (1, 'Some string'), (2, 'Some other string'), (3, 'random'), (4, 'random2');

SELECT * FROM 02416_rocksdb_delete ORDER BY key;
SELECT '-----------';

DELETE FROM 02416_rocksdb_delete WHERE value LIKE 'Some%string';

SELECT * FROM 02416_rocksdb_delete ORDER BY key;
SELECT '-----------';

ALTER TABLE 02416_rocksdb_delete DELETE WHERE key >= 4;

SELECT * FROM 02416_rocksdb_delete ORDER BY key;

DROP TABLE IF EXISTS 02416_rocksdb_delete;
