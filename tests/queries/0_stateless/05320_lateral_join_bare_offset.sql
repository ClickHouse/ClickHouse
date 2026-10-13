-- A bare `OFFSET` (without `LIMIT`) inside a `LATERAL` subquery skips rows within each
-- evaluation of the subquery, not within the combined result.

SET enable_analyzer = 1;
SET allow_experimental_lateral_join = 1;

DROP TABLE IF EXISTS outer_t;
DROP TABLE IF EXISTS inner_t;

CREATE TABLE outer_t (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE inner_t (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;

INSERT INTO outer_t VALUES (1, 10), (2, 20), (3, 30), (4, 40);
INSERT INTO inner_t VALUES (10, 1), (10, 2), (10, 3), (20, 4), (20, 5), (40, 6);

SELECT 'left, offset 1';
SELECT o.id, l.v FROM outer_t o
LEFT JOIN LATERAL (SELECT v FROM inner_t WHERE inner_t.k = o.k ORDER BY v OFFSET 1) AS l ON true
ORDER BY o.id, l.v;

SELECT 'inner, offset 1';
SELECT o.id, l.v FROM outer_t o
INNER JOIN LATERAL (SELECT v FROM inner_t WHERE inner_t.k = o.k ORDER BY v DESC OFFSET 1) AS l ON true
ORDER BY o.id, l.v;

SELECT 'inner, offset 0';
SELECT o.id, l.v FROM outer_t o
INNER JOIN LATERAL (SELECT v FROM inner_t WHERE inner_t.k = o.k ORDER BY v OFFSET 0) AS l ON true
ORDER BY o.id, l.v;

SELECT 'inner, offset 2';
SELECT o.id, l.v FROM outer_t o
INNER JOIN LATERAL (SELECT v FROM inner_t WHERE inner_t.k = o.k ORDER BY v OFFSET 2 ROWS) AS l ON true
ORDER BY o.id, l.v;

SELECT o.id, l.v FROM outer_t o
INNER JOIN LATERAL (SELECT v FROM inner_t WHERE inner_t.k = o.k ORDER BY v OFFSET -1) AS l ON true
ORDER BY o.id, l.v; -- { serverError NOT_IMPLEMENTED }

SELECT o.id, l.v FROM outer_t o
INNER JOIN LATERAL (SELECT v FROM inner_t WHERE inner_t.k = o.k ORDER BY v OFFSET 0.5) AS l ON true
ORDER BY o.id, l.v; -- { serverError NOT_IMPLEMENTED }

DROP TABLE outer_t;
DROP TABLE inner_t;
