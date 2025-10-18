DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int) ENGINE = Join(ALL, FULL, c0);
INSERT INTO t0 VALUES (1);
SELECT 1 FROM (SELECT 1) tx FULL JOIN t0 USING () SETTINGS allow_experimental_parallel_reading_from_replicas = 1; -- { serverError INVALID_JOIN_ON_EXPRESSION }
DROP TABLE t0;
