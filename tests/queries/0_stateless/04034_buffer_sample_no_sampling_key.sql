-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/99134
-- SAMPLE on a Buffer table backed by a MergeTree without SAMPLE BY used to crash the server.

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

CREATE TABLE t0 (c0 Int) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t1 (c0 Int) ENGINE = Buffer(currentDatabase(), t0, 1, 0, 0, 1, 1, 1, 1);

INSERT INTO t0 VALUES (1);

SELECT 1 FROM t1 SAMPLE 0.1; -- { serverError SAMPLING_NOT_SUPPORTED }

DROP TABLE t1;
DROP TABLE t0;

-- Buffer table without a destination table should support SAMPLE (reads only from in-memory buffers).
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (c0 Int) ENGINE = Buffer('', '', 1, 3600, 86400, 100, 1000000, 10000000, 100000000);
INSERT INTO t2 SELECT number FROM numbers(100);
SELECT count() >= 0 FROM t2 SAMPLE 0.5;
DROP TABLE t2;
