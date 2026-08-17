-- Tags: no-parallel
-- ^ creates a database

CREATE DATABASE IF NOT EXISTS d1;

CREATE TABLE IF NOT EXISTS d1.t1 (val Int) engine=Memory;
INSERT INTO d1.t1 SELECT 1;

SELECT * FROM t1; -- { serverError UNKNOWN_TABLE }

USE DATABASE d1;

SELECT * FROM t1;

DROP TABLE t1;
DROP DATABASE d1;

CREATE DATABASE IF NOT EXISTS database;

USE DATABASE database;
USE database;

DROP DATABASE database;
