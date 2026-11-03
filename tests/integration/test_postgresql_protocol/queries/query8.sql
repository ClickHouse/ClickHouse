CREATE DATABASE IF NOT EXISTS db_psql_describe;
USE db_psql_describe;
CREATE TABLE t_described (x Int32) ENGINE = Memory;
CREATE VIEW v_described AS SELECT x FROM t_described;
\d
\dt
DROP DATABASE db_psql_describe;
