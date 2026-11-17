-- A LATERAL subquery is evaluated once per distinct value of the correlated columns, so a volatile function
-- in a hidden filter of a table read inside it (a row policy or `additional_table_filters`) is rejected
-- the same way as a volatile function written in the subquery itself.

SET enable_analyzer = 1;
SET allow_experimental_lateral_join = 1;

DROP ROW POLICY IF EXISTS 05319_policy ON inner_policy_t;
DROP TABLE IF EXISTS outer_t;
DROP TABLE IF EXISTS inner_t;
DROP TABLE IF EXISTS inner_policy_t;

CREATE TABLE outer_t (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE inner_t (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE inner_policy_t (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;

INSERT INTO outer_t VALUES (1, 10), (2, 10), (3, 20);
INSERT INTO inner_t VALUES (10, 1), (10, 2), (20, 3);
INSERT INTO inner_policy_t VALUES (10, 1), (10, 2), (20, 3);

SELECT '-- additional_table_filters';
SELECT o.id, l.c FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM inner_t WHERE inner_t.k = o.k) AS l ON true
SETTINGS additional_table_filters = {'inner_t': 'rand() % 2 = 0'}; -- { serverError NOT_IMPLEMENTED }

SELECT o.id, l.c FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM (SELECT k FROM inner_t) AS s WHERE s.k = o.k) AS l ON true
SETTINGS additional_table_filters = {'inner_t': 'rand() % 2 = 0'}; -- { serverError NOT_IMPLEMENTED }

-- Deterministic additional filters are allowed.
SELECT o.id, l.c FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM inner_t WHERE inner_t.k = o.k) AS l ON true
ORDER BY o.id
SETTINGS additional_table_filters = {'inner_t': 'v > 1'};

-- A volatile filter on the outer table only is not evaluated inside the LATERAL subquery.
SELECT count() FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM inner_t WHERE inner_t.k = o.k) AS l ON true
SETTINGS additional_table_filters = {'outer_t': 'rand() % 2 = 0 OR 1'};

SELECT '-- row policy';
CREATE ROW POLICY 05319_policy ON inner_policy_t USING rand() % 2 = 0 OR v > 0 TO ALL;

SELECT o.id, l.c FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM inner_policy_t WHERE inner_policy_t.k = o.k) AS l ON true; -- { serverError NOT_IMPLEMENTED }

DROP ROW POLICY 05319_policy ON inner_policy_t;
CREATE ROW POLICY 05319_policy ON inner_policy_t USING v > 1 TO ALL;

SELECT o.id, l.c FROM outer_t o
LEFT JOIN LATERAL (SELECT count() AS c FROM inner_policy_t WHERE inner_policy_t.k = o.k) AS l ON true
ORDER BY o.id;

DROP ROW POLICY 05319_policy ON inner_policy_t;
DROP TABLE outer_t;
DROP TABLE inner_t;
DROP TABLE inner_policy_t;
