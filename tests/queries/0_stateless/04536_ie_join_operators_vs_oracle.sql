-- Tags: no-old-analyzer

-- All 16 combinations of inequality operators on a duplicate-heavy fixture, verified
-- against a brute-force cross-join oracle. The oracle queries use the comma-join syntax
-- with the conditions in WHERE, which is executed as a cross join with a filter
-- (`cross_to_inner_join_rewrite` is disabled so that they cannot be routed through IEJoin).

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t1 (id Int32, x Int32, y Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (id Int32, x Int32, y Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t1 VALUES (1, 1, 3), (2, 1, 2), (3, 2, 2), (4, 2, 1), (5, 3, 1);
INSERT INTO t2 VALUES (1, 1, 1), (2, 1, 3), (3, 2, 2), (4, 3, 2), (5, 3, 3);

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id, r.id FROM t1 l JOIN t2 r ON l.x < r.x AND l.y < r.y) WHERE explain LIKE '%IEJoin%';

SELECT '<' AS op1, '<' AS op2, (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l JOIN t2 r ON l.x < r.x AND l.y < r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l, t2 r WHERE (l.x < r.x) AND (l.y < r.y)) AS ok, (SELECT count() FROM t1 l JOIN t2 r ON l.x < r.x AND l.y < r.y) AS cnt;
SELECT '<' AS op1, '<=' AS op2, (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l JOIN t2 r ON l.x < r.x AND l.y <= r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l, t2 r WHERE (l.x < r.x) AND (l.y <= r.y)) AS ok, (SELECT count() FROM t1 l JOIN t2 r ON l.x < r.x AND l.y <= r.y) AS cnt;
SELECT '<' AS op1, '>' AS op2, (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l JOIN t2 r ON l.x < r.x AND l.y > r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l, t2 r WHERE (l.x < r.x) AND (l.y > r.y)) AS ok, (SELECT count() FROM t1 l JOIN t2 r ON l.x < r.x AND l.y > r.y) AS cnt;
SELECT '<' AS op1, '>=' AS op2, (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l JOIN t2 r ON l.x < r.x AND l.y >= r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l, t2 r WHERE (l.x < r.x) AND (l.y >= r.y)) AS ok, (SELECT count() FROM t1 l JOIN t2 r ON l.x < r.x AND l.y >= r.y) AS cnt;
SELECT '<=' AS op1, '<' AS op2, (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l JOIN t2 r ON l.x <= r.x AND l.y < r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l, t2 r WHERE (l.x <= r.x) AND (l.y < r.y)) AS ok, (SELECT count() FROM t1 l JOIN t2 r ON l.x <= r.x AND l.y < r.y) AS cnt;
SELECT '<=' AS op1, '<=' AS op2, (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l JOIN t2 r ON l.x <= r.x AND l.y <= r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l, t2 r WHERE (l.x <= r.x) AND (l.y <= r.y)) AS ok, (SELECT count() FROM t1 l JOIN t2 r ON l.x <= r.x AND l.y <= r.y) AS cnt;
SELECT '<=' AS op1, '>' AS op2, (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l JOIN t2 r ON l.x <= r.x AND l.y > r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l, t2 r WHERE (l.x <= r.x) AND (l.y > r.y)) AS ok, (SELECT count() FROM t1 l JOIN t2 r ON l.x <= r.x AND l.y > r.y) AS cnt;
SELECT '<=' AS op1, '>=' AS op2, (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l JOIN t2 r ON l.x <= r.x AND l.y >= r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l, t2 r WHERE (l.x <= r.x) AND (l.y >= r.y)) AS ok, (SELECT count() FROM t1 l JOIN t2 r ON l.x <= r.x AND l.y >= r.y) AS cnt;
SELECT '>' AS op1, '<' AS op2, (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l JOIN t2 r ON l.x > r.x AND l.y < r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l, t2 r WHERE (l.x > r.x) AND (l.y < r.y)) AS ok, (SELECT count() FROM t1 l JOIN t2 r ON l.x > r.x AND l.y < r.y) AS cnt;
SELECT '>' AS op1, '<=' AS op2, (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l JOIN t2 r ON l.x > r.x AND l.y <= r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l, t2 r WHERE (l.x > r.x) AND (l.y <= r.y)) AS ok, (SELECT count() FROM t1 l JOIN t2 r ON l.x > r.x AND l.y <= r.y) AS cnt;
SELECT '>' AS op1, '>' AS op2, (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l JOIN t2 r ON l.x > r.x AND l.y > r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l, t2 r WHERE (l.x > r.x) AND (l.y > r.y)) AS ok, (SELECT count() FROM t1 l JOIN t2 r ON l.x > r.x AND l.y > r.y) AS cnt;
SELECT '>' AS op1, '>=' AS op2, (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l JOIN t2 r ON l.x > r.x AND l.y >= r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l, t2 r WHERE (l.x > r.x) AND (l.y >= r.y)) AS ok, (SELECT count() FROM t1 l JOIN t2 r ON l.x > r.x AND l.y >= r.y) AS cnt;
SELECT '>=' AS op1, '<' AS op2, (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l JOIN t2 r ON l.x >= r.x AND l.y < r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l, t2 r WHERE (l.x >= r.x) AND (l.y < r.y)) AS ok, (SELECT count() FROM t1 l JOIN t2 r ON l.x >= r.x AND l.y < r.y) AS cnt;
SELECT '>=' AS op1, '<=' AS op2, (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l JOIN t2 r ON l.x >= r.x AND l.y <= r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l, t2 r WHERE (l.x >= r.x) AND (l.y <= r.y)) AS ok, (SELECT count() FROM t1 l JOIN t2 r ON l.x >= r.x AND l.y <= r.y) AS cnt;
SELECT '>=' AS op1, '>' AS op2, (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l JOIN t2 r ON l.x >= r.x AND l.y > r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l, t2 r WHERE (l.x >= r.x) AND (l.y > r.y)) AS ok, (SELECT count() FROM t1 l JOIN t2 r ON l.x >= r.x AND l.y > r.y) AS cnt;
SELECT '>=' AS op1, '>=' AS op2, (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l JOIN t2 r ON l.x >= r.x AND l.y >= r.y) = (SELECT arraySort(groupArray((l.id, r.id))) FROM t1 l, t2 r WHERE (l.x >= r.x) AND (l.y >= r.y)) AS ok, (SELECT count() FROM t1 l JOIN t2 r ON l.x >= r.x AND l.y >= r.y) AS cnt;

DROP TABLE t1;
DROP TABLE t2;
