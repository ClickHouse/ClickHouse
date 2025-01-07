DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS b;

CREATE TABLE a (key Nullable(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO a VALUES (NULL), ('1');

CREATE TABLE b (key Nullable(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO b VALUES (NULL), ('1');

SELECT a.key FROM a LEFT SEMI JOIN b ON tuple(a.key) = tuple(b.key) ORDER BY a.key;
SELECT a.key FROM a LEFT SEMI JOIN b ON a.key IS NOT DISTINCT FROM b.key ORDER BY a.key;
SELECT a.key FROM a LEFT SEMI JOIN b ON tuple(a.key) = tuple(b.key) ORDER BY a.key;
SELECT a.key FROM a LEFT ANY JOIN b ON tuple(a.key) = tuple(b.key) ORDER BY a.key;

DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS b;
