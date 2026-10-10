-- A LATERAL subquery is evaluated once per distinct value of the correlated columns, so tables whose
-- engine samples new rows on every read (`GenerateRandom`, `FuzzQuery`, `FuzzJSON`) are rejected,
-- like the corresponding table functions.

SET enable_analyzer = 1;
SET allow_experimental_lateral_join = 1;

DROP TABLE IF EXISTS outer_t;
DROP TABLE IF EXISTS random_t;
DROP TABLE IF EXISTS fuzz_query_t;

CREATE TABLE outer_t (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO outer_t VALUES (1, 10), (2, 10), (3, 20);

CREATE TABLE random_t (x UInt32) ENGINE = GenerateRandom(1);
CREATE TABLE fuzz_query_t (q String) ENGINE = FuzzQuery('SELECT 1', 500, 1);

SELECT o.id, l.r FROM outer_t o
LEFT JOIN LATERAL (SELECT x + o.k AS r FROM random_t LIMIT 1) AS l ON true; -- { serverError NOT_IMPLEMENTED }

SELECT o.id, l.c FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM (SELECT x FROM random_t LIMIT 10) WHERE x > o.k) AS l ON true; -- { serverError NOT_IMPLEMENTED }

SELECT o.id, l.q FROM outer_t o
LEFT JOIN LATERAL (SELECT concat(q, toString(o.k)) AS q FROM fuzz_query_t LIMIT 1) AS l ON true; -- { serverError NOT_IMPLEMENTED }

-- A deterministic table is still accepted.
SELECT o.id, l.r FROM outer_t o
LEFT JOIN LATERAL (SELECT number + o.k AS r FROM numbers(1)) AS l ON true
ORDER BY o.id;

DROP TABLE fuzz_query_t;
DROP TABLE random_t;
DROP TABLE outer_t;
