-- { echo ON }

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS v;

CREATE TABLE t ( x String, y String, v Float64 ) ENGINE=MergeTree ORDER BY (x, y);
ALTER TABLE t ADD PROJECTION tp ( SELECT x, SUM(v) GROUP BY x );
CREATE VIEW v AS SELECT * FROM t;
INSERT INTO t (x, y, v) SELECT number % 10, number % 100, number FROM numbers(10000);

SET optimize_use_projections = 1;
SET force_optimize_projection = 1;
EXPLAIN SELECT x, SUM(v) FROM v GROUP BY x;
SELECT x, SUM(v) FROM v GROUP BY x ORDER BY x;

DROP TABLE t;
DROP TABLE v;
