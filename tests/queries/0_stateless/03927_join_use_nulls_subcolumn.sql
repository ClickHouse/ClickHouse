-- Tags: no-fasttest
-- https://github.com/ClickHouse/ClickHouse/issues/90902
SET enable_analyzer = 1;
SET join_use_nulls = 1;

CREATE TABLE t0 (c0 String) ENGINE = EmbeddedRocksDB() PRIMARY KEY (c0);

SELECT 1 FROM t0 FULL JOIN t0 t1 ON t0.c0 = t1.c0 WHERE t0.c0.size = 1;

SELECT 1 FROM t0 LEFT JOIN t0 t1 ON t0.c0 = t1.c0 WHERE t1.c0.size = 1;

SELECT 1 FROM t0 RIGHT JOIN t0 t1 ON t0.c0 = t1.c0 WHERE t0.c0.size = 1;

INSERT INTO t0 VALUES ('hello');

SELECT t0.c0.size FROM t0 FULL JOIN t0 t1 ON t0.c0 = t1.c0;
