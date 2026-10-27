-- A LATERAL subquery is evaluated once per distinct value of the correlated columns, so table functions
-- that generate random rows are rejected like non-deterministic functions. The final LATERAL join matches
-- all evaluations at once, so its size limits are always enforced with THROW, also without a buffer.

SET enable_analyzer = 1;
SET allow_experimental_lateral_join = 1;

DROP TABLE IF EXISTS outer_t;
DROP TABLE IF EXISTS inner_t;

CREATE TABLE outer_t (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE inner_t (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;

INSERT INTO outer_t VALUES (1, 10), (2, 10), (3, 20);
INSERT INTO inner_t VALUES (10, 1), (20, 2);

SELECT '-- volatile table functions';
SELECT o.id, l.r FROM outer_t o
LEFT JOIN LATERAL (SELECT x + o.k AS r FROM generateRandom('x UInt8') LIMIT 1) AS l ON true; -- { serverError NOT_IMPLEMENTED }

SELECT o.id, l.s FROM outer_t o
LEFT JOIN LATERAL (SELECT sum(x) AS s FROM (SELECT x FROM generateRandom('x UInt8') LIMIT 3) WHERE x > o.k) AS l ON true; -- { serverError NOT_IMPLEMENTED }

-- Deterministic table functions are allowed.
SELECT o.id, l.c FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM numbers(5) WHERE number < o.k % 7) AS l ON true
ORDER BY o.id;

SELECT '-- join overflow without buffer';
SET correlated_subqueries_use_in_memory_buffer = 0;

SELECT o.id, l.v FROM outer_t o
INNER JOIN LATERAL (SELECT v FROM inner_t i WHERE i.k = o.k) AS l ON true
ORDER BY o.id
SETTINGS max_rows_in_join = 1000, join_overflow_mode = 'break';

SELECT o.id, l.v FROM outer_t o
INNER JOIN LATERAL (SELECT v FROM inner_t i WHERE i.k = o.k) AS l ON true
SETTINGS max_rows_in_join = 1, join_overflow_mode = 'break'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT o.id, l.c FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM inner_t i WHERE i.k = o.k) AS l ON true
SETTINGS max_rows_in_join = 1, join_overflow_mode = 'break'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

DROP TABLE outer_t;
DROP TABLE inner_t;
