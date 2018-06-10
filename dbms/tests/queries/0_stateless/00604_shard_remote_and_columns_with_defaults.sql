DROP TABLE IF EXISTS test.t1;
DROP TABLE IF EXISTS test.t2;
DROP TABLE IF EXISTS test.t3;
DROP TABLE IF EXISTS test.t4;

CREATE TABLE test.t1(x UInt32, y UInt32) ENGINE TinyLog;
CREATE TABLE test.t2(x UInt32, y UInt32 DEFAULT x + 1) ENGINE TinyLog;
CREATE TABLE test.t3(x UInt32, y UInt32 MATERIALIZED x + 1) ENGINE TinyLog;
CREATE TABLE test.t4(x UInt32, y UInt32 ALIAS x + 1) ENGINE TinyLog;

INSERT INTO test.t1 VALUES (1, 1);
INSERT INTO test.t2 VALUES (1, 1);
INSERT INTO test.t3 VALUES (1);
INSERT INTO test.t4 VALUES (1);

INSERT INTO FUNCTION remote('127.0.0.2', test.t1) VALUES (2, 2);
INSERT INTO FUNCTION remote('127.0.0.2', test.t2) VALUES (2, 2);
--TODO: INSERT into remote tables with MATERIALIZED columns.
--INSERT INTO FUNCTION remote('127.0.0.2', test.t3) VALUES (2);
INSERT INTO FUNCTION remote('127.0.0.2', test.t4) VALUES (2);

SELECT * FROM remote('127.0.0.2', test.t1) ORDER BY x;

SELECT '*** With a DEFAULT column ***';
SELECT * FROM remote('127.0.0.2', test.t2) ORDER BY x;

SELECT '*** With a MATERIALIZED column ***';
SELECT * FROM remote('127.0.0.2', test.t3) ORDER BY x;
SELECT x, y FROM remote('127.0.0.2', test.t3) ORDER BY x;

SELECT '*** With an ALIAS column ***';
SELECT * FROM remote('127.0.0.2', test.t4) ORDER BY x;
SELECT x, y FROM remote('127.0.0.2', test.t4) ORDER BY x;

DROP TABLE test.t1;
DROP TABLE test.t2;
DROP TABLE test.t3;
DROP TABLE test.t4;
