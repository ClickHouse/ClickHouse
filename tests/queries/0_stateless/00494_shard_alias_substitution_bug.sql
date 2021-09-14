DROP TABLE IF EXISTS nested;
CREATE TABLE nested (n Nested(x UInt8)) ENGINE = Memory;
INSERT INTO nested VALUES ([1, 2]);
SELECT 1 AS x FROM remote('127.0.0.2', currentDatabase(), nested) ARRAY JOIN n.x;
DROP TABLE nested;

SELECT dummy AS dummy, dummy AS b FROM system.one;
SELECT dummy AS dummy, dummy AS b, b AS c FROM system.one;
SELECT b AS c, dummy AS b, dummy AS dummy FROM system.one;
