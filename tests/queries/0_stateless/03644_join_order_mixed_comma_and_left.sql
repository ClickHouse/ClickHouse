DROP TABLE IF EXISTS n1;
DROP TABLE IF EXISTS n2;
DROP TABLE IF EXISTS n3;

SET query_plan_optimize_join_order_limit=16;

CREATE TABLE n1 (number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO n1 SELECT number FROM numbers(3);

CREATE TABLE n2 (number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO n2 SELECT number FROM numbers(2);

CREATE TABLE n3 (number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO n3 SELECT number FROM numbers(2);

SELECT * FROM n1, n2 LEFT JOIN n3 ON n1.number = n3.number ORDER BY n1.number, n2.number, n3.number;

INSERT INTO n2 SELECT number FROM numbers(4);
SELECT * FROM n1, n2 LEFT JOIN n3 ON n1.number = n3.number ORDER BY n1.number, n2.number, n3.number;

INSERT INTO n3 SELECT number FROM numbers(4);
SELECT * FROM n1, n2 LEFT JOIN n3 ON n1.number = n3.number ORDER BY n1.number, n2.number, n3.number;

