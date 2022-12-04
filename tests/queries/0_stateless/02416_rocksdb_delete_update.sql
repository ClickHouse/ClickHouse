-- Tags: no-ordinary-database, no-fasttest

DROP TABLE IF EXISTS _02416_rocksdb;

CREATE TABLE _02416_rocksdb (key UInt64, value String, value2 UInt64) Engine=EmbeddedRocksDB PRIMARY KEY(key);

INSERT INTO _02416_rocksdb VALUES (1, 'Some string', 0), (2, 'Some other string', 0), (3, 'random', 0), (4, 'random2', 0);

SELECT * FROM _02416_rocksdb ORDER BY key;
SELECT '-----------';

DELETE FROM _02416_rocksdb WHERE value LIKE 'Some%string';

SELECT * FROM _02416_rocksdb ORDER BY key;
SELECT '-----------';

ALTER TABLE _02416_rocksdb DELETE WHERE key >= 4;

SELECT * FROM _02416_rocksdb ORDER BY key;
SELECT '-----------';

DELETE FROM _02416_rocksdb WHERE 1 = 1;
SELECT count() FROM _02416_rocksdb;
SELECT '-----------';

INSERT INTO _02416_rocksdb VALUES (1, 'String', 10), (2, 'String', 20), (3, 'String', 30), (4, 'String', 40);
SELECT * FROM _02416_rocksdb ORDER BY key;
SELECT '-----------';

ALTER TABLE _02416_rocksdb UPDATE value = 'Another' WHERE key > 2;
SELECT * FROM _02416_rocksdb ORDER BY key;
SELECT '-----------';

ALTER TABLE _02416_rocksdb UPDATE key = key * 10 WHERE 1 = 1; -- { serverError 36 }
SELECT * FROM _02416_rocksdb ORDER BY key;
SELECT '-----------';

ALTER TABLE _02416_rocksdb UPDATE value2 = value2 * 10 + 2 WHERE 1 = 1;
SELECT * FROM _02416_rocksdb ORDER BY key;
SELECT '-----------';

DROP TABLE IF EXISTS _02416_rocksdb;
