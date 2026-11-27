DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (c1 Int32, c2 Int32) ENGINE MergeTree ORDER BY c1;
INSERT INTO t1 (c1, c2) VALUES (1, 10), (1, 20), (1, 30);

DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (c1 Int32, c2 Int32, c3 String) ENGINE MergeTree ORDER BY (c1, c2, c3);
INSERT INTO t2 (c1, c2, c3) VALUES (1, 5, 'a'), (1, 15, 'b'), (1, 25, 'c');

SET enable_optimize_predicate_expression = 1;
WITH
  v1 AS (SELECT t1.c2, t2.c2, t2.c3 FROM t1 ASOF JOIN t2 USING (c1, c2))
  SELECT count() FROM v1 WHERE c3 = 'b';

SET enable_optimize_predicate_expression = 0;
WITH
  v1 AS (SELECT t1.c2, t2.c2, t2.c3 FROM t1 ASOF JOIN t2 USING (c1, c2))
  SELECT count() FROM v1 WHERE c3 = 'b';
