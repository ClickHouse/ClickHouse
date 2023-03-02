-- Tags: no-ordinary-database, no-fasttest

DROP TABLE IF EXISTS 02661_keepermap_delete_update;

CREATE TABLE 02661_keepermap_delete_update (key UInt64, value String, value2 UInt64) ENGINE=KeeperMap('/' ||  currentDatabase() || '/test02661_keepermap_delete_update') PRIMARY KEY(key);

INSERT INTO 02661_keepermap_delete_update VALUES (1, 'Some string', 0), (2, 'Some other string', 0), (3, 'random', 0), (4, 'random2', 0);

SELECT * FROM 02661_keepermap_delete_update ORDER BY key;
SELECT '-----------';

DELETE FROM 02661_keepermap_delete_update WHERE value LIKE 'Some%string';

SELECT * FROM 02661_keepermap_delete_update ORDER BY key;
SELECT '-----------';

ALTER TABLE 02661_keepermap_delete_update DELETE WHERE key >= 4;

SELECT * FROM 02661_keepermap_delete_update ORDER BY key;
SELECT '-----------';

DELETE FROM 02661_keepermap_delete_update WHERE 1 = 1;
SELECT count() FROM 02661_keepermap_delete_update;
SELECT '-----------';

INSERT INTO 02661_keepermap_delete_update VALUES (1, 'String', 10), (2, 'String', 20), (3, 'String', 30), (4, 'String', 40);
SELECT * FROM 02661_keepermap_delete_update ORDER BY key;
SELECT '-----------';

ALTER TABLE 02661_keepermap_delete_update UPDATE value = 'Another' WHERE key > 2;
SELECT * FROM 02661_keepermap_delete_update ORDER BY key;
SELECT '-----------';

ALTER TABLE 02661_keepermap_delete_update UPDATE key = key * 10 WHERE 1 = 1; -- { serverError 36 }
SELECT * FROM 02661_keepermap_delete_update ORDER BY key;
SELECT '-----------';

ALTER TABLE 02661_keepermap_delete_update UPDATE value2 = value2 * 10 + 2 WHERE value2 < 100;
SELECT * FROM 02661_keepermap_delete_update ORDER BY key;
SELECT '-----------';

DROP TABLE IF EXISTS 02661_keepermap_delete_update;
