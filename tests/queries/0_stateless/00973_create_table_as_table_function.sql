DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;

CREATE TABLE t1 AS remote('127.0.0.1', system.one);
SELECT count() FROM t1;

CREATE TABLE t2 AS remote('127.0.0.1', system.numbers);
SELECT * FROM t2 LIMIT 18;

CREATE TABLE t3 AS remote('127.0.0.1', numbers(100));
SELECT * FROM t3 where number > 17 and number < 25;

CREATE TABLE t4 AS numbers(100);
SELECT count() FROM t4 where number > 74;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
