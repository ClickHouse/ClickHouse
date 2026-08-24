DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Float64, c1 BFloat16) ENGINE = MergeTree() PARTITION BY (murmurHash3_64(c0)) PRIMARY KEY (c0);
SET join_algorithm = 'full_sorting_merge';
INSERT INTO TABLE t0 (c0, c1) VALUES (4, -1), (-1745033997, 7), (-1940737579, nan);
SELECT count() FROM t0 t0d0 JOIN t0 t1d0 ON t1d0.c0 = t0d0.c0 JOIN t0 t2d0 ON t1d0.c1 = t2d0.c1 WHERE t1d0.c0 != t2d0.c0;
SELECT sum(t1d0.c0 != t2d0.c0) FROM t0 t0d0 JOIN t0 t1d0 ON t1d0.c0 = t0d0.c0 JOIN t0 t2d0 ON t1d0.c1 = t2d0.c1;
DROP TABLE t0;
