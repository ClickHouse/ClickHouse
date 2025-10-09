DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int) ENGINE = Join(ALL, FULL, c0);
INSERT INTO t0 VALUES (1);
SELECT 1 FROM (SELECT 1) tx FULL JOIN t0 USING () SETTINGS allow_experimental_parallel_reading_from_replicas = 1; -- { serverError INVALID_JOIN_ON_EXPRESSION }
DROP TABLE t0;

CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO TABLE t0 (c0) VALUES (1);
SELECT 1 FROM t0 JOIN t0 t1 ON t0.c0 = t1.c0 SETTINGS allow_experimental_parallel_reading_from_replicas = 0;
SELECT 1 FROM t0 JOIN t0 t1 ON t0.c0 = t1.c0 SETTINGS parallel_replicas_mode = 'custom_key_sampling', allow_experimental_parallel_reading_from_replicas = 1, enable_parallel_replicas = 1;
DROP TABLE t0;
