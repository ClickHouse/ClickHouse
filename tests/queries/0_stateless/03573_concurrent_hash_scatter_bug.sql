
DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (i Int64, j Int16) ENGINE = MergeTree ORDER BY i;
INSERT INTO t0 VALUES (1, 1), (2, 2);

SET join_algorithm = 'parallel_hash';

SELECT i FROM t0 ANY JOIN (SELECT 3 AS k) AS x ON x.k = j;

INSERT INTO t0 SELECT number, number FROM numbers_mt(100_000) WHERE number != 3;
SELECT i FROM t0 ANY JOIN (SELECT 3 AS k) AS x ON x.k = j;
