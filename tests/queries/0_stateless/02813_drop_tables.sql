DROP DATABASE IF EXISTS drop_tables_test;
CREATE DATABASE IF NOT EXISTS drop_tables_test;
CREATE TABLE drop_tables_test.t1 (id UInt32) Engine=Memory();
CREATE TABLE drop_tables_test.t2 (id UInt32) Engine=Memory();
CREATE TABLE drop_tables_test.t3 (id UInt32) Engine=Memory();
CREATE TABLE drop_tables_test.t4 (id UInt32) Engine=Memory();
CREATE TABLE drop_tables_test.t5 (id UInt32) Engine=Memory();
CREATE TABLE drop_tables_test.t6 (id UInt32) Engine=Memory();
CREATE TABLE drop_tables_test.t7 (id UInt32) Engine=Memory();
CREATE TABLE drop_tables_test.t8 (id UInt32) Engine=Memory();

SELECT 'CREATE tables t1,t2,t3,t4,t5,t6,t7,t8';
SHOW TABLES FROM drop_tables_test;

DROP TABLE drop_tables_test.t1,drop_tables_test.t2;
SELECT 'DROPPED t1,t2';
SHOW TABLES FROM drop_tables_test;

USE drop_tables_test;
DROP TABLE t3,t4;
SELECT 'DROPPED t3,t4';
SHOW TABLES FROM drop_tables_test;

DROP TABLE drop_tables_test.t5,t6;
SELECT 'DROPPED t5,t6';
SHOW TABLES FROM drop_tables_test;

DROP TABLE t7,drop_tables_test.t8;
SELECT 'DROPPED t7,t8';
SHOW TABLES FROM drop_tables_test;

DROP DATABASE IF EXISTS drop_tables_test;
