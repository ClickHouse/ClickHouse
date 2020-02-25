select count() > 1 as ok from (select * from odbc('DSN={ClickHouse DSN (ANSI)}','system','tables'));
select count() > 1 as ok from (select * from odbc('DSN={ClickHouse DSN (Unicode)}','system','tables'));

DROP DATABASE IF EXISTS test_01086;
CREATE DATABASE test_01086;
USE test_01086;

CREATE TABLE t (x UInt8, y Float32, z String) ENGINE = Memory;
INSERT INTO t VALUES (1,0.1,'a'),(2,0.2,'b'),(3,0.3,'c');

select * from odbc('DSN={ClickHouse DSN (ANSI)}','test_01086','t') ORDER BY x;

DROP DATABASE test_01086;
