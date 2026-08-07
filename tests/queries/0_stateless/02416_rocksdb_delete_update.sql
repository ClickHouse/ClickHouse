-- Tags: no-ordinary-database, no-fasttest, use-rocksdb

DROP TABLE IF EXISTS 02416_rocksdb;

CREATE TABLE 02416_rocksdb (key UInt64, value String, value2 UInt64) Engine=EmbeddedRocksDB PRIMARY KEY(key);

INSERT INTO 02416_rocksdb VALUES (1, 'Some string', 0), (2, 'Some other string', 0), (3, 'random', 0), (4, 'random2', 0);

SELECT * FROM 02416_rocksdb ORDER BY key;
SELECT '-----------';

DELETE FROM 02416_rocksdb WHERE value LIKE 'Some%string';

SELECT * FROM 02416_rocksdb ORDER BY key;
SELECT '-----------';

ALTER TABLE 02416_rocksdb DELETE WHERE key >= 4;

SELECT * FROM 02416_rocksdb ORDER BY key;
SELECT '-----------';

DELETE FROM 02416_rocksdb WHERE 1 = 1;
SELECT count() FROM 02416_rocksdb;
SELECT '-----------';

INSERT INTO 02416_rocksdb VALUES (1, 'String', 10), (2, 'String', 20), (3, 'String', 30), (4, 'String', 40);
SELECT * FROM 02416_rocksdb ORDER BY key;
SELECT '-----------';

ALTER TABLE 02416_rocksdb UPDATE value = 'Another' WHERE key > 2;
SELECT * FROM 02416_rocksdb ORDER BY key;
SELECT '-----------';

ALTER TABLE 02416_rocksdb UPDATE key = key * 10 WHERE 1 = 1; -- { serverError BAD_ARGUMENTS }
SELECT * FROM 02416_rocksdb ORDER BY key;
SELECT '-----------';

ALTER TABLE 02416_rocksdb UPDATE value2 = value2 * 10 + 2 WHERE 1 = 1;
SELECT * FROM 02416_rocksdb ORDER BY key;
SELECT '-----------';

DROP TABLE IF EXISTS 02416_rocksdb;
