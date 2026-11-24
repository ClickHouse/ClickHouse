-- A LATERAL subquery is evaluated once per distinct value of the correlated columns, so reads that run
-- a script (the `executable` table function, `Executable` and `ExecutablePool` tables) are rejected,
-- like functions that are non-deterministic within a query.

SET enable_analyzer = 1;
SET allow_experimental_lateral_join = 1;

DROP TABLE IF EXISTS outer_t;
DROP TABLE IF EXISTS exec_t;
DROP TABLE IF EXISTS exec_pool_t;

CREATE TABLE outer_t (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO outer_t VALUES (1, 10), (2, 10), (3, 20);

CREATE TABLE exec_t (x UInt32) ENGINE = Executable('nonexist.sh', 'TSV');
CREATE TABLE exec_pool_t (x UInt32) ENGINE = ExecutablePool('nonexist.sh', 'TSV');

SELECT o.id, l.r FROM outer_t o
LEFT JOIN LATERAL (SELECT x + o.k AS r FROM executable('nonexist.sh', 'TSV', 'x UInt32')) AS l ON true; -- { serverError NOT_IMPLEMENTED }

SELECT o.id, l.r FROM outer_t o
LEFT JOIN LATERAL (SELECT x + o.k AS r FROM exec_t) AS l ON true; -- { serverError NOT_IMPLEMENTED }

SELECT o.id, l.r FROM outer_t o
LEFT JOIN LATERAL (SELECT x + o.k AS r FROM exec_pool_t) AS l ON true; -- { serverError NOT_IMPLEMENTED }

SELECT o.id, l.s FROM outer_t o
LEFT JOIN LATERAL (SELECT sum(x) AS s FROM (SELECT x FROM exec_t) WHERE x > o.k) AS l ON true; -- { serverError NOT_IMPLEMENTED }

DROP TABLE exec_pool_t;
DROP TABLE exec_t;
DROP TABLE outer_t;
