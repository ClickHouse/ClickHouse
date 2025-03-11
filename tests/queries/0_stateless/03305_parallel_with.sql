DROP TABLE IF EXISTS table1
PARALLEL WITH
DROP TABLE IF EXISTS table2
  PARALLEL WITH
DROP TABLE IF EXISTS table3
  PARALLEL WITH
DROP VIEW IF EXISTS mv_table3;

CREATE TABLE table1(x Int32) ENGINE=MergeTree order by x
PARALLEL WITH
CREATE TABLE table2(y Int32) ENGINE=MergeTree order by y;

SHOW CREATE TABLE table1;
SHOW CREATE TABLE table2;

CREATE TABLE table1(x Int32) ENGINE=MergeTree order by x
PARALLEL WITH
CREATE TABLE table2(y Int32) ENGINE=MergeTree order by y; -- { serverError TABLE_ALREADY_EXISTS }

INSERT INTO table1 SELECT number FROM numbers(5, 1)
PARALLEL WITH
CREATE TABLE table3(y Int32) ENGINE=MergeTree order by y;

SHOW CREATE TABLE table3;

INSERT INTO table1 SELECT number FROM numbers(3)
PARALLEL WITH
INSERT INTO table1 SELECT number FROM numbers(10, 2)
PARALLEL WITH
INSERT INTO table2 SELECT number FROM numbers(20, 1);

-- insert will not complete because SELECT fails
INSERT INTO table3(y) SELECT number FROM numbers(30, 3)
PARALLEL WITH
SELECT * FROM table1 ORDER BY x; -- { serverError INCORRECT_QUERY }

INSERT INTO table3(y) SELECT number FROM numbers(30, 3)
PARALLEL WITH
CREATE MATERIALIZED VIEW mv_table3
ENGINE = MergeTree
ORDER BY y AS
SELECT
    y,
    y * 10 AS y_times_ten
FROM table3;

INSERT INTO table3 SELECT number FROM numbers(30, 3)
PARALLEL WITH
ALTER TABLE table3 ADD COLUMN z String DEFAULT 'unknown';

SELECT 'table1:';
SELECT * FROM table1 ORDER BY x;
SELECT 'table2:';
SELECT * FROM table2 ORDER BY y;
SELECT 'table3:';
SELECT * FROM table3 ORDER BY y;
SELECT 'mv_table3:';
SELECT * FROM mv_table3 ORDER BY y;

DROP TABLE table1
PARALLEL WITH
DROP TABLE table2;

INSERT INTO table3 SELECT number, 'test' FROM numbers(30, 3)
PARALLEL WITH
DROP TABLE table3; -- { serverError UNKNOWN_TABLE }

INSERT INTO table3 SELECT number, 'test' FROM numbers(30, 3)
PARALLEL WITH
DROP TABLE table3; -- { serverError UNKNOWN_TABLE }

SELECT 'tables exist:';
EXISTS TABLE table1;
EXISTS TABLE table2;
EXISTS TABLE table3;
EXISTS TABLE table3;

DROP VIEW IF EXISTS mv_table3
