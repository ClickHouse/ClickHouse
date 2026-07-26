DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS b;
DROP TABLE IF EXISTS c;

CREATE TABLE a (id UInt64, x UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE b (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE c (x UInt64) ENGINE = MergeTree ORDER BY x;

INSERT INTO a SELECT number AS id, number % 5 AS x FROM numbers(10);
INSERT INTO b SELECT number FROM numbers(10);
INSERT INTO c SELECT number % 5 AS x FROM numbers(10);

SET enable_analyzer = 1;
SET enable_join_transitive_predicates = 1;

SET query_plan_optimize_join_order_randomize = 1;

-- Correct answer is 100: every one of the 10x10 a×c pairs is preserved by the LEFT JOIN
-- (matched with b when a.x=c.x and a.id=b.id, NULL-extended otherwise).
SELECT 'reorder ON ' AS tag, count()
FROM a CROSS JOIN c
LEFT JOIN b ON a.id = b.id AND a.x = c.x
SETTINGS query_plan_optimize_join_order_limit = 10;

-- Reference value with reorder disabled.
SELECT 'reorder OFF' AS tag, count()
FROM a CROSS JOIN c
LEFT JOIN b ON a.id = b.id AND a.x = c.x
SETTINGS query_plan_optimize_join_order_limit = 0;
