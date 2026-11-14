-- A volatile `additional_table_filters` predicate is rejected inside a LATERAL subquery not only for a plain
-- table but also for a table function, because the read-planning path attaches it to both.

SET enable_analyzer = 1;
SET allow_experimental_lateral_join = 1;

DROP TABLE IF EXISTS outer_t;
DROP TABLE IF EXISTS inner_t;

CREATE TABLE outer_t (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE inner_t (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;

INSERT INTO outer_t VALUES (1, 10), (2, 10), (3, 20);
INSERT INTO inner_t VALUES (10, 1), (10, 2), (20, 3);

SELECT o.id, l.c FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM merge(currentDatabase(), '^inner_t$') AS m WHERE m.k = o.k) AS l ON true
SETTINGS additional_table_filters = {'m': 'rand() % 2 = 0'}; -- { serverError NOT_IMPLEMENTED }

SELECT o.id, l.c FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM numbers(30) AS n WHERE n.number = o.k) AS l ON true
SETTINGS additional_table_filters = {'n': 'rand() % 2 = 0'}; -- { serverError NOT_IMPLEMENTED }

-- The filter is matched by the alias of a plain table as well.
SELECT o.id, l.c FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM inner_t AS i WHERE i.k = o.k) AS l ON true
SETTINGS additional_table_filters = {'i': 'rand() % 2 = 0'}; -- { serverError NOT_IMPLEMENTED }

-- Deterministic additional filters on a table function are allowed.
SELECT o.id, l.c FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM merge(currentDatabase(), '^inner_t$') AS m WHERE m.k = o.k) AS l ON true
ORDER BY o.id
SETTINGS additional_table_filters = {'m': 'v > 1'};

DROP TABLE outer_t;
DROP TABLE inner_t;
