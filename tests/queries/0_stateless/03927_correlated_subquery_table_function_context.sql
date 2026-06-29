-- Tags: no-fasttest, no-parallel-replicas
-- Correlated subquery with a table function should not fail with "Context has expired".
-- https://github.com/ClickHouse/ClickHouse/issues/92991

SET enable_analyzer = 1;

CREATE TABLE t0_03927 (c0 Int) ENGINE = Memory();
INSERT INTO t0_03927 VALUES (1);

SELECT (SELECT t0_03927.c0 FROM url('http://localhost:8123/?query=SELECT+1', 'CSV') tx) FROM t0_03927;
SELECT * FROM t0_03927 WHERE EXISTS (
    SELECT 1 FROM url('http://localhost:8123/?query=SELECT+1', 'CSV') tx 
    WHERE tx.c1 = t0_03927.c0
);
DROP TABLE t0_03927;
