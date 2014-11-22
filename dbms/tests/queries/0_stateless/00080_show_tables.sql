DROP DATABASE IF EXISTS test_show_tables;

CREATE DATABASE test_show_tables;

CREATE TABLE test_show_tables.A (A UInt8) ENGINE = TinyLog;
CREATE TABLE test_show_tables.B (A UInt8) ENGINE = TinyLog;

SHOW TABLES from test_show_tables;

DROP DATABASE test_show_tables;
