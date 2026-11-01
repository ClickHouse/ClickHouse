-- { echo ON }

SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS v;

CREATE TABLE t ( x String, y String, v Float64 ) ENGINE=MergeTree ORDER BY (x, y);
ALTER TABLE t ADD PROJECTION tp ( SELECT x, SUM(v) GROUP BY x );
CREATE VIEW v AS SELECT * FROM t;
INSERT INTO t (x, y, v) SELECT number % 10, number % 100, number FROM numbers(10000);

SET optimize_use_projections = 1;
SET force_optimize_projection = 1;
SELECT x, SUM(v) FROM v GROUP BY x ORDER BY x;

DROP TABLE t;
DROP TABLE v;
