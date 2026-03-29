DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t5;
DROP TABLE IF EXISTS t6;
DROP TABLE IF EXISTS t7;

CREATE TABLE t1 AS remote('127.0.0.1', system.one);
SELECT count() FROM t1;

CREATE TABLE t2 AS remote('127.0.0.1', system.numbers);
SELECT * FROM t2 LIMIT 18;

CREATE TABLE t3 AS remote('127.0.0.1', numbers(100));
SELECT * FROM t3 where number > 17 and number < 25;

CREATE TABLE t4 AS numbers(100);
SELECT count() FROM t4 where number > 74;

CREATE TABLE t5 ENGINE = MergeTree AS numbers(100);
SELECT count() FROM t5;
SELECT engine, create_table_query FROM system.tables WHERE name = 't5' AND database = currentDatabase();

CREATE TABLE t6 AS numbers(100) ENGINE = Memory;
SELECT count() FROM t6;
SELECT engine, create_table_query FROM system.tables WHERE name = 't6' AND database = currentDatabase();

CREATE TABLE t7 AS numbers(10) ORDER BY number; -- { serverError 119 }
CREATE TABLE t7 ORDER BY number AS numbers(10); -- { serverError 119 }
CREATE TABLE t7 (number UInt64) AS numbers(10) ENGINE = Memory; -- { serverError 122 }
CREATE TABLE t7 (number UInt64) ENGINE = Memory AS numbers(10); -- { serverError 122 }

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;
DROP TABLE t6;
