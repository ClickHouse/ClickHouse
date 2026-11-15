-- An unspecified strictness of `LATERAL JOIN` means `ALL` and must not depend on
-- `join_default_strictness` or `any_join_distinct_right_table_keys`.

SET enable_analyzer = 1;
SET allow_experimental_lateral_join = 1;

DROP TABLE IF EXISTS outer_t;
DROP TABLE IF EXISTS inner_t;

CREATE TABLE outer_t (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE inner_t (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;

INSERT INTO outer_t VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO inner_t VALUES (10, 1), (10, 2), (20, 3);

SELECT 'any';
SELECT o.id, l.v FROM outer_t o
LEFT JOIN LATERAL (SELECT v FROM inner_t WHERE inner_t.k = o.k) AS l ON true
ORDER BY o.id, l.v
SETTINGS join_default_strictness = 'ANY';

SELECT o.id, l.v FROM outer_t o
INNER JOIN LATERAL (SELECT v FROM inner_t WHERE inner_t.k = o.k) AS l ON true
ORDER BY o.id, l.v
SETTINGS join_default_strictness = 'ANY';

SELECT 'any, distinct right table keys';
SELECT o.id, l.v FROM outer_t o
LEFT JOIN LATERAL (SELECT v FROM inner_t WHERE inner_t.k = o.k) AS l ON true
ORDER BY o.id, l.v
SETTINGS join_default_strictness = 'ANY', any_join_distinct_right_table_keys = 1;

SELECT o.id, l.v FROM outer_t o
INNER JOIN LATERAL (SELECT v FROM inner_t WHERE inner_t.k = o.k) AS l ON true
ORDER BY o.id, l.v
SETTINGS join_default_strictness = 'ANY', any_join_distinct_right_table_keys = 1;

SELECT 'empty';
SELECT o.id, l.v FROM outer_t o
LEFT JOIN LATERAL (SELECT v FROM inner_t WHERE inner_t.k = o.k) AS l ON true
ORDER BY o.id, l.v
SETTINGS join_default_strictness = '';

-- An explicit unsupported strictness is still rejected.
SELECT o.id, l.v FROM outer_t o
LEFT ANY JOIN LATERAL (SELECT v FROM inner_t WHERE inner_t.k = o.k) AS l ON true; -- { serverError NOT_IMPLEMENTED }

DROP TABLE outer_t;
DROP TABLE inner_t;
