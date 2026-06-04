-- https://github.com/ClickHouse/ClickHouse/issues/42399
SET enable_analyzer=1;

CREATE TABLE IF NOT EXISTS t0 (c0 Int32) ENGINE = Memory() ;
CREATE TABLE t1 (c0 Int32, c1 Int32, c2 Int32) ENGINE = Memory() ;
CREATE TABLE t2 (c0 String, c1 String MATERIALIZED (c2), c2 Int32) ENGINE = Memory() ;
CREATE TABLE t3 (c0 String, c1 String, c2 String) ENGINE = Log() ;
CREATE TABLE IF NOT EXISTS t4 (c0 Int32) ENGINE = Log() ;
SELECT t3.c1, t3.c2, t1.c1, t1.c0, t2.c2, t0.c0, t1.c2, t2.c1, t4.c0 FROM t3, t0, t1, t2, t4;
