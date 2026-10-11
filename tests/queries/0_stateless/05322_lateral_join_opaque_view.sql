-- A view that the analyzer does not inline is planned only when it is read, so a volatile function in its
-- body cannot be detected. Such a view is rejected inside a LATERAL subquery; an inlined view is checked
-- like any other subquery.

SET enable_analyzer = 1;
SET allow_experimental_lateral_join = 1;

DROP VIEW IF EXISTS v_volatile;
DROP VIEW IF EXISTS v_plain;
DROP VIEW IF EXISTS v_param;
DROP TABLE IF EXISTS outer_t;
DROP TABLE IF EXISTS inner_t;

CREATE TABLE outer_t (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE inner_t (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;

INSERT INTO outer_t VALUES (1, 10), (2, 10), (3, 20);
INSERT INTO inner_t VALUES (10, 1), (10, 2), (20, 3);

CREATE VIEW v_volatile AS SELECT k, v, rand() AS r FROM inner_t;
CREATE VIEW v_plain AS SELECT k, v FROM inner_t;
CREATE VIEW v_param AS SELECT k, v FROM inner_t WHERE v >= {min_v:UInt32};

SET analyzer_inline_views = 0;

SELECT o.id, l.r FROM outer_t o
LEFT JOIN LATERAL (SELECT r FROM v_volatile AS v WHERE v.k = o.k LIMIT 1) AS l ON true; -- { serverError NOT_IMPLEMENTED }

SELECT o.id, l.c FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM v_plain AS v WHERE v.k = o.k) AS l ON true; -- { serverError NOT_IMPLEMENTED }

SELECT o.id, l.c FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM v_param(min_v = 2) AS v WHERE v.k = o.k) AS l ON true; -- { serverError NOT_IMPLEMENTED }

-- A view nested in a subquery of the LATERAL subquery is found as well.
SELECT o.id, l.c FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM (SELECT * FROM v_plain) AS v WHERE v.k = o.k) AS l ON true; -- { serverError NOT_IMPLEMENTED }

SET analyzer_inline_views = 1;

-- An inlined view with a volatile function is rejected by the regular check.
SELECT o.id, l.r FROM outer_t o
LEFT JOIN LATERAL (SELECT r FROM v_volatile AS v WHERE v.k = o.k LIMIT 1) AS l ON true; -- { serverError NOT_IMPLEMENTED }

-- An inlined deterministic view is allowed.
SELECT o.id, l.c FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM v_plain AS v WHERE v.k = o.k) AS l ON true
ORDER BY o.id;

DROP VIEW v_volatile;
DROP VIEW v_plain;
DROP VIEW v_param;
DROP TABLE outer_t;
DROP TABLE inner_t;
