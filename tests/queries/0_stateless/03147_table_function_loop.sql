SELECT * FROM loop(numbers(5));

DROP DATABASE IF EXISTS 03147_db;
CREATE DATABASE 03147_db;
CREATE TABLE 03147_db.t (n Int8) ENGINE=MergeTree ORDER BY n;
INSERT INTO 03147_db.t SELECT * FROM numbers(10);

SELECT * FROM loop(03147_db.t);
SELECT * FROM loop(mysql('127.0.0.1:9004', currentDatabase(), t, 'default', ''));