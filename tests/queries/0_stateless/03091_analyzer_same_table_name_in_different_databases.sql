-- https://github.com/ClickHouse/ClickHouse/issues/61947
SET allow_experimental_analyzer=1;

DROP DATABASE IF EXISTS d1;
DROP DATABASE IF EXISTS d2;

CREATE DATABASE d1;
CREATE DATABASE d2;
CREATE TABLE d1.`1-1` (field Int8) ENGINE = Memory;
CREATE TABLE d2.`1-1` (field Int8) ENGINE = Memory;
CREATE TABLE d2.`2-1` (field Int8) ENGINE = Memory;

INSERT INTO d1.`1-1` VALUES (1);

SELECT *
FROM d1.`1-1`
LEFT JOIN d2.`1-1` ON d1.`1-1`.field = d2.`1-1`.field;

SELECT '';

SELECT 'using asterisk', d1.`1-1`.*, d2.`1-1`.*
FROM d1.`1-1`
LEFT JOIN d2.`1-1` USING field
UNION ALL
SELECT 'using field name', d1.`1-1`.field, d2.`1-1`.field
FROM d1.`1-1`
LEFT JOIN d2.`1-1` USING field
ORDER BY *;
