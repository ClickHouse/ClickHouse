-- https://github.com/ClickHouse/ClickHouse/issues/61947
SET allow_experimental_analyzer=1;

DROP DATABASE IF EXISTS d3;
DROP DATABASE IF EXISTS d4;

CREATE DATABASE d3;
CREATE DATABASE d4;
CREATE TABLE d3.`1-1` (field Int8) ENGINE = Memory;
CREATE TABLE d4.`2-1` (field Int8) ENGINE = Memory;
CREATE TABLE d4.`3-1` (field Int8) ENGINE = Memory;

INSERT INTO d3.`1-1` VALUES (1);

SELECT d3.`1-1`.*
FROM d3.`1-1`
LEFT JOIN d4.`2-1` ON d3.`1-1`.field = d4.`2-1`.field
LEFT JOIN d4.`3-1` ON d4.`2-1`.field = d4.`3-1`.field;
