DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table3;
DROP TABLE IF EXISTS table4;
DROP TABLE IF EXISTS table2;

CREATE TABLE table1 (id UInt64, val Nullable(UInt64)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE table3 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE table4 (v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE table2 (id UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO table1 VALUES (1, 1), (2, NULL);
INSERT INTO table3 VALUES (1), (2);
INSERT INTO table4 VALUES (1);
INSERT INTO table2 VALUES (1), (2);

SET query_plan_optimize_join_order_limit = 16;

SELECT table1.id
FROM table1
INNER JOIN table2 ON table1.id = table2.id
INNER JOIN table3 ON table1.id = table3.id
LEFT JOIN table4 ON (table1.val = table4.v) AND (table3.id = table4.v)
ORDER BY table1.id
;
