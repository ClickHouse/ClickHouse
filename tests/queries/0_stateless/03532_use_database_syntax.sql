CREATE DATABASE IF NOT EXISTS d1;

CREATE TABLE d1.t1 (val Int) engine=Memory;
INSERT INTO d1.t1 SELECT 1;

select * from t1; -- { serverError UNKNOWN_TABLE }

USE DATABASE d1;

select * from t1;

DROP DATABASE d1;
