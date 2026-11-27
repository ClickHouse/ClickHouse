-- https://github.com/ClickHouse/ClickHouse/issues/77558
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int, c1 Int ALIAS 1) ENGINE = Memory;
SELECT t0.c1 FROM t0 ORDER BY 1;
DROP TABLE IF EXISTS t0;
