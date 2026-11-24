-- A LATERAL subquery is evaluated once per distinct value of the correlated columns, not once per
-- left row, so functions that are non-deterministic within a query are rejected: two left rows with
-- the same correlated value would otherwise share one result.

SET enable_analyzer = 1;
SET allow_experimental_lateral_join = 1;

DROP TABLE IF EXISTS outer_t;
DROP TABLE IF EXISTS inner_t;

CREATE TABLE outer_t (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE inner_t (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;

INSERT INTO outer_t VALUES (1, 10), (2, 10), (3, 20);
INSERT INTO inner_t VALUES (10, 1), (10, 2), (20, 3);

SELECT o.id, l.r FROM outer_t o
LEFT JOIN LATERAL (SELECT rand() + o.k AS r) AS l ON true; -- { serverError NOT_IMPLEMENTED }

SELECT o.id, l.r FROM outer_t o
INNER JOIN LATERAL (SELECT generateUUIDv4() AS r FROM inner_t i WHERE i.k = o.k) AS l ON true; -- { serverError NOT_IMPLEMENTED }

SELECT o.id, l.s FROM outer_t o
LEFT JOIN LATERAL (SELECT sum(v) AS s FROM inner_t i WHERE i.k = o.k AND i.v > rand() % 1) AS l ON true; -- { serverError NOT_IMPLEMENTED }

SELECT o.id, l.s FROM outer_t o
LEFT JOIN LATERAL (SELECT sum(n) AS s FROM (SELECT rowNumberInAllBlocks() AS n FROM inner_t i WHERE i.k = o.k)) AS l ON true; -- { serverError NOT_IMPLEMENTED }

-- Functions that are constant within a query are allowed.
SELECT o.id, l.s, l.ok FROM outer_t o
LEFT JOIN LATERAL (SELECT sum(v) AS s, now() >= toDateTime('2000-01-01') AS ok FROM inner_t i WHERE i.k = o.k) AS l ON true
ORDER BY o.id;

DROP TABLE outer_t;
DROP TABLE inner_t;
