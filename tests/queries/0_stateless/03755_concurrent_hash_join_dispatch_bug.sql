-- Test for concurrent hash join with ON FALSE or missing key columns see https://github.com/ClickHouse/ClickHouse/issues/91173

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Int) ENGINE = Memory;
SELECT 1 FROM remote('localhost', currentDatabase(), t0) AS t0	
JOIN t0 t1 ON FALSE	
RIGHT JOIN t0 t2 ON FALSE;	

DROP TABLE t0;
