-- Tags: no-parallel
DROP DATABASE IF EXISTS 02961_db1;
CREATE DATABASE IF NOT EXISTS 02961_db1;
DROP DATABASE IF EXISTS 02961_db2;
CREATE DATABASE IF NOT EXISTS 02961_db2;


CREATE TABLE IF NOT EXISTS 02961_db1.02961_tb1 (id UInt32) Engine=Memory();
CREATE TABLE IF NOT EXISTS 02961_db1.02961_tb2 (id UInt32) Engine=Memory();

CREATE TABLE IF NOT EXISTS 02961_db2.02961_tb3 (id UInt32) Engine=Memory();
CREATE TABLE IF NOT EXISTS 02961_db2.02961_tb4 (id UInt32) Engine=Memory();
CREATE TABLE IF NOT EXISTS 02961_db2.02961_tb5 (id UInt32) Engine=Memory();

DROP TABLE 02961_db1.02961_tb1, 02961_db1.02961_tb2, 02961_db2.02961_tb3;

SELECT '-- check which tables exist in 02961_db1';
SHOW TABLES FROM 02961_db1;
SELECT '-- check which tables exist in 02961_db2';
SHOW TABLES FROM 02961_db2;

SELECT 'Test when deletion of existing table fails';
DROP TABLE 02961_db2.02961_tb4, 02961_db1.02961_tb1, 02961_db2.02961_tb5; -- { serverError UNKNOWN_TABLE }

SELECT '-- check which tables exist in 02961_db1';
SHOW TABLES FROM 02961_db1;
SELECT '-- check which tables exist in 02961_db2';
SHOW TABLES FROM 02961_db2;

DROP TABLE IF EXISTS tab1, tab2, tab3;
CREATE TABLE IF NOT EXISTS tab1 (id UInt32) Engine=Memory();
CREATE TABLE IF NOT EXISTS tab2 (id UInt32) Engine=Memory();
CREATE TABLE IF NOT EXISTS tab3 (id UInt32) Engine=Memory();

INSERT INTO tab2 SELECT number FROM system.numbers limit 10;

DROP TABLE IF EMPTY tab1, tab2, tab3; -- { serverError TABLE_NOT_EMPTY }
SELECT 'Test when deletion of not empty table fails';
SHOW TABLES;

TRUNCATE TABLE tab2, tab3; -- { clientError SYNTAX_ERROR }

DROP TABLE IF EXISTS tab1, tab2, tab3;

DROP DATABASE IF EXISTS 02961_db1;
DROP DATABASE IF EXISTS 02961_db2;
